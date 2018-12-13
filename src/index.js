'use strict'

const Block = require('ipfs-block')
const CID = require('cids')
const IPFSRepo = require('ipfs-repo')
const BlockService = require('ipfs-block-service')
const series = require('async/series')
const waterfall = require('async/waterfall')
const MemoryStore = require('interface-datastore').MemoryDatastore
const mergeOptions = require('merge-options')
const ipldDagCbor = require('ipld-dag-cbor')
const ipldDagPb = require('ipld-dag-pb')
const ipldRaw = require('ipld-raw')
const multicodec = require('multicodec')
const typical = require('typical')
const { fancyIterator } = require('./util')

function noop () {}

class IPLDResolver {
  constructor (userOptions) {
    const options = mergeOptions(IPLDResolver.defaultOptions, userOptions)

    if (!options.blockService) {
      throw new Error('Missing blockservice')
    }
    this.bs = options.blockService

    // Object with current list of active resolvers
    this.resolvers = {}

    if (options.loadFormat === undefined) {
      this.loadFormat = async (codec) => {
        const codecName = this._codecName(codec)
        throw new Error(`No resolver found for codec "${codecName}"`)
      }
    } else {
      this.loadFormat = options.loadFormat
    }

    // Enable all supplied formats
    for (const format of options.formats) {
      this.addFormat(format)
    }
  }

  /**
   * Add support for an IPLD Format.
   *
   * @param {Object} format - The implementation of an IPLD Format.
   * @returns {void}
   */
  addFormat (format) {
    const codec = this._codecFromName(format.resolver.multicodec)
    if (this.resolvers[codec]) {
      const codecName = this._codecName(format.resolver.multicodec)
      throw new Error(`Resolver already exists for codec "${codecName}"`)
    }

    this.resolvers[codec] = {
      resolver: format.resolver,
      util: format.util
    }
  }

  /**
   * Remove support for an IPLD Format.
   *
   * @param {number} codec - The codec of the IPLD Format to remove.
   * @returns {void}
   */
  removeFormat (codec) {
    if (this.resolvers[codec]) {
      delete this.resolvers[codec]
    }
  }

  /**
   * Retrieves IPLD Nodes along the `path` that is rooted at `cid`.
   *
   * @param {CID} cid - the CID the resolving starts.
   * @param {string} path - the path that should be resolved.
   * @returns {Iterable.<Promise.<{remainderPath: string, value}>>} - Returns an async iterator of all the IPLD Nodes that were traversed during the path resolving. Every element is an object with these fields:
   *   - `remainderPath`: the part of the path that wasn’t resolved yet.
   *   - `value`: the value where the resolved path points to. If further traversing is possible, then the value is a CID object linking to another IPLD Node. If it was possible to fully resolve the path, value is the value the path points to. So if you need the CID of the IPLD Node you’re currently at, just take the value of the previously returned IPLD Node.
   */
  resolve (cid, path) {
    if (!CID.isCID(cid)) {
      throw new Error('`cid` argument must be a CID')
    }
    if (typeof path !== 'string') {
      throw new Error('`path` argument must be a string')
    }

    const resolveIterator = {
      next: () => {
        // End iteration if there isn't a CID to follow anymore
        if (cid === null) {
          return {
            done: true
          }
        }

        const iterValue = new Promise(async (resolve, reject) => {
          let format
          try {
            const codec = this._codecFromName(cid.codec)
            format = await this._getFormat(codec)
          } catch (err) {
            return reject(err)
          }

          // get block
          // use local resolver
          // update path value
          this.bs.get(cid, (err, block) => {
            if (err) {
              return reject(err)
            }

            format.resolver.resolve(block.data, path, (err, result) => {
              if (err) {
                return reject(err)
              }

              // Prepare for the next iteration if there is a `remainderPath`
              path = result.remainderPath
              let value = result.value
              // NOTE vmx 2018-11-29: Not all IPLD Formats return links as
              // CIDs yet. Hence try to convert old style links to CIDs
              if (Object.keys(value).length === 1 && '/' in value) {
                value = new CID(value['/'])
              }
              if (CID.isCID(value)) {
                cid = value
              } else {
                cid = null
              }

              return resolve({
                remainderPath: path,
                value
              })
            })
          })
        })

        return {
          value: iterValue,
          done: false
        }
      }
    }

    return fancyIterator(resolveIterator)
  }

  /**
   * Get multiple nodes back from an array of CIDs.
   *
   * @param {Iterable.<CID>} cids - The CIDs of the IPLD Nodes that should be retrieved.
   * @returns {Iterable.<Promise.<Object>>} - Returns an async iterator with the IPLD Nodes that correspond to the given `cids`.
   */
  get (cids) {
    if (!typical.isIterable(cids) || typical.isString(cids) ||
        Buffer.isBuffer(cids)) {
      throw new Error('`cids` must be an iterable of CIDs')
    }

    let blocks
    const getIterator = {
      next: () => {
        // End of iteration if there aren't any blocks left to return
        if (cids.length === 0 ||
          (blocks !== undefined && blocks.length === 0)
        ) {
          return {
            done: true
          }
        }
        const iterValue = new Promise(async (resolve, reject) => {
          // Lazy load block.
          // Currntly the BlockService return all nodes as an array. In the
          // future this will also be an iterator
          if (blocks === undefined) {
            const cidsArray = Array.from(cids)
            this.bs.getMany(cidsArray, async (err, returnedBlocks) => {
              if (err) {
                return reject(err)
              }
              blocks = returnedBlocks
              const block = blocks.shift()
              try {
                const node = await this._deserialize(block)
                return resolve(node)
              } catch (err) {
                return reject(err)
              }
            })
          } else {
            const block = blocks.shift()
            try {
              const node = await this._deserialize(block)
              return resolve(node)
            } catch (err) {
              return reject(err)
            }
          }
        })

        return {
          value: iterValue,
          done: false
        }
      }
    }
    return fancyIterator(getIterator)
  }

  /**
   * Stores the given IPLD Nodes of a recognized IPLD Format.
   *
   * @param {Iterable.<Object>} nodes - Deserialized IPLD nodes that should be inserted.
   * @param {Object} userOptions -  Options are applied to any of the `nodes` and is an object with the following properties.
   * @param {number} userOptions.format - the multicodec of the format that IPLD Node should be encoded in.
   * @param {number} [userOtions.hashAlg=hash algorithm of the given multicodec] - The hashing algorithm that is used to calculate the CID.
   * @param {number} [userOptions.version=1]`- The CID version to use.
   * @param {boolean} [userOptions.onlyHash=false] - If true the serialized form of the IPLD Node will not be passed to the underlying block store.
   * @returns {Iterable.<Promise.<CID>>} - Returns an async iterator with the CIDs of the serialized IPLD Nodes.
   */
  put (nodes, userOptions) {
    if (!typical.isIterable(nodes) || typical.isString(nodes) ||
        Buffer.isBuffer(nodes)) {
      throw new Error('`nodes` must be an iterable')
    }
    if (userOptions === undefined) {
      throw new Error('`put` requires options')
    }
    if (userOptions.format === undefined) {
      throw new Error('`format` option must be set')
    }
    if (typeof userOptions.format !== 'number') {
      throw new Error('`format` option must be number (multicodec)')
    }

    let options
    let format

    const putIterator = {
      next: () => {
        // End iteration if there are no more nodes to put
        if (nodes.length === 0) {
          return {
            done: true
          }
        }

        const iterValue = new Promise(async (resolve, reject) => {
          // Lazy load the options not when the iterator is initialized, but
          // when we hit the first iteration. This way the constructor can be
          // a synchronous function.
          if (options === undefined) {
            try {
              format = await this._getFormat(userOptions.format)
            } catch (err) {
              return reject(err)
            }
            const defaultOptions = {
              hashAlg: format.defaultHashAlg,
              version: 1,
              onlyHash: false
            }
            options = mergeOptions(defaultOptions, userOptions)
          }

          const node = nodes.shift()
          format.util.cid(node, options, (err, cid) => {
            if (err) {
              return reject(err)
            }

            if (options.onlyHash) {
              return resolve(cid)
            }

            this._put(cid, node, (err, cid) => {
              if (err) {
                return reject(err)
              }
              return resolve(cid)
            })
          })
        })

        return {
          value: iterValue,
          done: false
        }
      }
    }
    return fancyIterator(putIterator)
  }

  /**
   * Remove IPLD Nodes by the given CIDs.
   *
   * Throws an error if any of the Blocks can’t be removed. This operation is
   * *not* atomic, some Blocks might have already been removed.
   *
   * @param {Iterable.<CID>} cids - The CIDs of the IPLD Nodes that should be removed
   * @return {void}
   */
  remove (cids) {
    if (!typical.isIterable(cids) || typical.isString(cids) ||
        Buffer.isBuffer(cids)) {
      throw new Error('`cids` must be an iterable of CIDs')
    }

    const removeIterator = {
      next: () => {
        // End iteration if there are no more nodes to remove
        if (cids.length === 0) {
          return {
            done: true
          }
        }

        const iterValue = new Promise((resolve, reject) => {
          const cid = cids.shift()
          this.bs.delete(cid, (err) => {
            if (err) {
              return reject(err)
            }
            return resolve(cid)
          })
        })

        return {
          value: iterValue,
          done: false
        }
      }
    }
    return fancyIterator(removeIterator)
  }

  /*           */
  /* internals */
  /*           */
  async _getFormat (codec) {
    if (this.resolvers[codec]) {
      return this.resolvers[codec]
    }

    // If not supported, attempt to dynamically load this format
    const format = await this.loadFormat(codec)
    this.resolvers[codec] = format
    return format
  }

  /**
   * Deserialize a given block
   *
   * @param {Object} block - The block to deserialize
   * @return {Object} = Returns the deserialized node
   */
  async _deserialize (block) {
    return new Promise((resolve, reject) => {
      const codec = this._codecFromName(block.cid.codec)
      this._getFormat(codec).then((format) => {
        // TODO vmx 2018-12-11: Make this one async/await once
        // `util.serialize()` is a Promise
        format.util.deserialize(block.data, (err, deserialized) => {
          if (err) {
            return reject(err)
          }
          return resolve(deserialized)
        })
      }).catch((err) => {
        return reject(err)
      })
    })
  }

  /**
   * Return the name that corresponds to a codec.
   *
   * NOTE: This is a dirty hack to make things work with the current
   * multicodec implementation. In the future the multicodec implementation
   * should use constants and have a way to return human friendly strings.
   *
   * @param {number} codec - The codec to get the name of.
   * @returns {string} - The name of the given codec.
   */
  _codecName (codec) {
    // const codecBuffer = multicodec.getCodeVarint(codec).readUInt8(0)
    // let hexString = codecBuffer.toString(16)
    let hexString = codec.toString(16)
    if (hexString.length % 2 === 1) {
      hexString = '0' + hexString
    }
    const codecName = multicodec.getCodec(Buffer.from(hexString, 'hex'))
    return codecName
  }

  /**
   * Return the codec based on the name.
   *
   * This is the reverse function of `_codecName()`.
   *
   * NOTE: This is a hack and it should really be replaced by a better
   * multicodec API.
   *
   * @param {string} name = The name of the codec.
   * @returns {number} codec = The coe of the given name.
   */
  _codecFromName (name) {
    const codecBuffer = multicodec.getCodeVarint(name)
    switch (codecBuffer.length) {
      case 1:
        return codecBuffer.readUInt8(0)
      case 2:
        return codecBuffer.readUInt16BE(0)
      default:
        // Not needed as other cases return
    }
  }

  _put (cid, node, callback) {
    callback = callback || noop

    waterfall([
      async () => {
        // TODO vmx 2018-12-12: Shouldn't be needed once all the code uses
        // the codec numbers instead of strings.
        const codec = this._codecFromName(cid.codec)
        return this._getFormat(codec)
      },
      (format, cb) => format.util.serialize(node, cb),
      (buf, cb) => this.bs.put(new Block(buf, cid), cb)
    ], (err) => {
      if (err) {
        return callback(err)
      }
      callback(null, cid)
    })
  }
}

/**
 * Default options for IPLD.
 */
IPLDResolver.defaultOptions = {
  formats: [ipldDagCbor, ipldDagPb, ipldRaw]
}

/**
 * Create an IPLD resolver with an in memory blockservice and
 * repo.
 *
 * @param {function(Error, IPLDResolver)} callback
 * @returns {void}
 */
IPLDResolver.inMemory = function (callback) {
  const repo = new IPFSRepo('in-memory', {
    storageBackends: {
      root: MemoryStore,
      blocks: MemoryStore,
      datastore: MemoryStore
    },
    lock: 'memory'
  })
  const blockService = new BlockService(repo)

  series([
    (cb) => repo.init({}, cb),
    (cb) => repo.open(cb)
  ], (err) => {
    if (err) {
      return callback(err)
    }
    callback(null, new IPLDResolver({ blockService }))
  })
}

module.exports = IPLDResolver
