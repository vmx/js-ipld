'use strict'

const Block = require('ipfs-block')
const pull = require('pull-stream')
const CID = require('cids')
const IPFSRepo = require('ipfs-repo')
const BlockService = require('ipfs-block-service')
const pullDeferSource = require('pull-defer').source
const pullTraverse = require('pull-traverse')
const map = require('async/map')
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

    // API entry point
    this.support = {}

    // Adds support for an IPLD format
    this.support.add = (multicodec, resolver, util) => {
      if (this.resolvers[multicodec]) {
        throw new Error('Resolver already exists for codec "' + multicodec + '"')
      }

      this.resolvers[multicodec] = {
        resolver: resolver,
        util: util
      }
    }

    if (options.loadFormat === undefined) {
      this.support.load = async (codec) => {
        throw new Error(`No resolver found for codec "${codec}"`)
      }
    } else {
      this.support.load = options.loadFormat
    }

    this.support.rm = (multicodec) => {
      if (this.resolvers[multicodec]) {
        delete this.resolvers[multicodec]
      }
    }

    // Enable all supplied formats
    for (const format of options.formats) {
      const { resolver, util } = format
      const multicodec = resolver.multicodec
      this.support.add(multicodec, resolver, util)
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
          return Promise.resolve({ done: true })
        }

        return new Promise(async (resolve, reject) => {
          let format
          try {
            format = await this._getFormat(cid.codec)
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
                done: false,
                value: {
                  remainderPath: path,
                  value
                }
              })
            })
          })
        })
      }
    }

    return fancyIterator(resolveIterator)
  }

  /**
   * Get multiple nodes back from an array of CIDs.
   *
   * @param {Array<CID>} cids
   * @param {function(Error, Array)} callback
   * @returns {void}
   */
  getMany (cids, callback) {
    if (!Array.isArray(cids)) {
      return callback(new Error('Argument must be an array of CIDs'))
    }
    this.bs.getMany(cids, (err, blocks) => {
      if (err) {
        return callback(err)
      }
      map(blocks, (block, mapCallback) => {
        // TODO vmx 2018-12-07: Make this one async/await once
        // `util.serialize()` is a Promise
        this._getFormat(block.cid.codec).then((format) => {
          format.util.deserialize(block.data, mapCallback)
        }).catch((err) => {
          mapCallback(err)
        })
      },
      callback)
    })
  }

  /*
   * Deserialize a given block
   *
   * @param {Object} block - The block to deserialize
   * @return {Object} = Returns the deserialized node
   */
  async _deserialize (block) {
    return new Promise((resolve, reject) => {
      this._getFormat(block.cid.codec).then((format) => {
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
          return Promise.resolve({ done: true })
        }

        return new Promise(async (resolve, reject) => {
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
                return resolve({ done: false, value: node })
              } catch (err) {
                return reject(err)
              }
            })
          } else {
            const block = blocks.shift()
            try {
              const node = await this._deserialize(block)
              return resolve({ done: false, value: node })
            } catch (err) {
              return reject(err)
            }
          }
        })
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
          return Promise.resolve({ done: true })
        }

        return new Promise(async (resolve, reject) => {
          // Lazy load the options not when the iterator is initialized, but
          // when we hit the first iteration. This way the constructor can be
          // a synchronous function.
          if (options === undefined) {
            // NOTE vmx 2018-12-07: This is a dirty hack to make things work with the
            // current multicodec implementations. Everything should be based on
            // constants and numbers and not on strings.
            let hexString = userOptions.format.toString(16)
            if (hexString.length % 2 === 1) {
              hexString = '0' + hexString
            }
            const formatCode = multicodec.getCodec(Buffer.from(hexString, 'hex'))
            try {
              format = await this._getFormat(formatCode)
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
              return resolve({ done: false, value: cid})
            }

            this._put(cid, node, (err, cid) => {
              if (err) {
                return reject(err)
              }
              return resolve({ done: false, value: cid})
            })
          })
        })
      }
    }
    return fancyIterator(putIterator)
  }

  treeStream (cid, path, options) {
    if (typeof path === 'object') {
      options = path
      path = undefined
    }

    options = options || {}

    let p

    if (!options.recursive) {
      p = pullDeferSource()

      waterfall([
        async () => {
          return this._getFormat(cid.codec)
        },
        (format, cb) => this.bs.get(cid, (err, block) => {
          if (err) return cb(err)
          cb(null, format, block)
        }),
        (format, block, cb) => format.resolver.tree(block.data, cb)
      ], (err, paths) => {
        if (err) {
          p.abort(err)
          return p
        }
        p.resolve(pull.values(paths))
      })
    }

    // recursive
    if (options.recursive) {
      p = pull(
        pullTraverse.widthFirst({
          basePath: null,
          cid: cid
        }, (el) => {
          // pass the paths through the pushable pull stream
          // continue traversing the graph by returning
          // the next cids with deferred

          if (typeof el === 'string') {
            return pull.empty()
          }

          const deferred = pullDeferSource()
          const cid = el.cid

          waterfall([
            async () => {
              return this._getFormat(cid.codec)
            },
            (format, cb) => this.bs.get(cid, (err, block) => {
              if (err) return cb(err)
              cb(null, format, block)
            }),
            (format, block, cb) => format.resolver.tree(block.data, (err, paths) => {
              if (err) {
                return cb(err)
              }
              map(paths, (p, cb) => {
                format.resolver.isLink(block.data, p, (err, link) => {
                  if (err) {
                    return cb(err)
                  }
                  cb(null, { path: p, link: link })
                })
              }, cb)
            })
          ], (err, paths) => {
            if (err) {
              deferred.abort(err)
              return deferred
            }

            deferred.resolve(pull.values(paths.map((p) => {
              const base = el.basePath ? el.basePath + '/' + p.path : p.path
              if (p.link) {
                return {
                  basePath: base,
                  cid: IPLDResolver._maybeCID(p.link)
                }
              }
              return base
            })))
          })
          return deferred
        }),
        pull.map((e) => {
          if (typeof e === 'string') {
            return e
          }
          return e.basePath
        }),
        pull.filter(Boolean)
      )
    }

    // filter out by path
    if (path) {
      return pull(
        p,
        pull.map((el) => {
          if (el.indexOf(path) === 0) {
            el = el.slice(path.length + 1)
            return el
          }
        }),
        pull.filter(Boolean)
      )
    }

    return p
  }

  remove (cids, callback) {
    this.bs.delete(cids, callback)
  }

  /*           */
  /* internals */
  /*           */
  async _getFormat (codec) {
    if (this.resolvers[codec]) {
      return this.resolvers[codec]
    }

    // If not supported, attempt to dynamically load this format
    const format = await this.support.load(codec)
    this.resolvers[codec] = format
    return format
  }

  _put (cid, node, callback) {
    callback = callback || noop

    waterfall([
      async () => {
        return this._getFormat(cid.codec)
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

  /**
   * Return a CID instance if it is a link.
   *
   * If something is a link `{"/": "baseencodedcid"}` or a CID, then return
   * a CID object, else return `null`.
   *
   * @param {*} link - The object to check
   * @returns {?CID} - A CID instance
   */
  static _maybeCID (link) {
    if (CID.isCID(link)) {
      return link
    }
    if (link && link['/'] !== undefined) {
      return new CID(link['/'])
    }
    return null
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
