/* eslint-env mocha */
'use strict'

/*
 * Test different types of data structures living together
 * &
 * Test data made of mixed data structures!
 */

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(dirtyChai)
const dagPB = require('ipld-dag-pb')
const dagCBOR = require('ipld-dag-cbor')
const waterfall = require('async/waterfall')
const CID = require('cids')
const multicodec = require('multicodec')

const IPLDResolver = require('../src')

describe('IPLD Resolver for dag-cbor + dag-pb', () => {
  let resolver

  let nodeCbor
  let nodePb
  let cidCbor
  let cidPb

  // TODO vmx 2018-12-07: Make multicodec use constants
  const formatDagPb = multicodec.getCodeVarint('dag-pb').readUInt8(0)
  const formatDagCbor = multicodec.getCodeVarint('dag-cbor').readUInt8(0)
  const hashAlgSha2 = multicodec.getCodeVarint('sha2-256').readUInt8(0)

  before((done) => {
    waterfall([
      (cb) => IPLDResolver.inMemory(cb),
      (res, cb) => {
        resolver = res
        dagPB.DAGNode.create(Buffer.from('I am inside a Protobuf'), cb)
      },
      (node, cb) => {
        nodePb = node
        dagPB.util.cid(nodePb, cb)
      },
      (cid, cb) => {
        cidPb = cid
        nodeCbor = {
          someData: 'I am inside a Cbor object',
          pb: cidPb
        }

        dagCBOR.util.cid(nodeCbor, cb)
      },
      async (cid, cb) => {
        const resultPb = resolver.put([nodePb], {
          format: formatDagPb, version: 0
        })
        cidPb = await resultPb.next().value
        const resultCbor = resolver.put([nodeCbor], { format: formatDagCbor })
        cidCbor = await resultCbor.next().value
      }
    ], done)
  })

  it('resolve through different formats', async () => {
    const result = resolver.resolve(cidCbor, 'pb/Data')

    const node1 = await result.next().value
    expect(node1.remainderPath).to.eql('Data')
    expect(node1.value).to.eql(cidPb)

    const node2 = await result.next().value
    expect(node2.remainderPath).to.eql('')
    expect(node2.value).to.eql(Buffer.from('I am inside a Protobuf'))
  })

  it('does not store nodes when onlyHash is passed', (done) => {
    waterfall([
      (cb) => dagPB.DAGNode.create(Buffer.from('Some data here'), cb),
      async (node) => {
        const result = resolver.put([node], {
          onlyHash: true,
          version: 1,
          hashAlg: hashAlgSha2,
          format: formatDagPb
        })
        return result.first()
      },
      (cid, cb) => resolver.bs._repo.blocks.has(cid, cb)
    ], (error, result) => {
      if (error) {
        return done(error)
      }

      expect(result).to.be.false()
      done()
    })
  })

  describe('getMany', () => {
    it('should return nodes correctly', (done) => {
      resolver.getMany([cidCbor, cidPb], (err, result) => {
        expect(err).to.not.exist()
        expect(result.length).to.equal(2)
        expect(result).to.deep.equal([nodeCbor, nodePb])
        done()
      })
    })

    it('should return nodes in input order', (done) => {
      resolver.getMany([cidPb, cidCbor], (err, result) => {
        expect(err).to.not.exist()
        expect(result.length).to.equal(2)
        expect(result).to.deep.equal([nodePb, nodeCbor])
        done()
      })
    })

    it('should return error on invalid CID', (done) => {
      resolver.getMany([cidCbor, 'invalidcid'], (err, result) => {
        expect(err.message).to.equal('Not a valid cid')
        expect(result).to.be.undefined()
        done()
      })
    })

    it('should return error on non-existent CID', (done) => {
      const nonExistentCid = new CID(
        'Qma4hjFTnCasJ8PVp3mZbZK5g2vGDT4LByLJ7m8ciyRFZP')
      resolver.getMany([cidCbor, nonExistentCid], (err, result) => {
        expect(err.message).to.equal('Not Found')
        expect(result).to.be.undefined()
        done()
      })
    })

    it('should return error on invalid input', (done) => {
      resolver.getMany('astring', (err, result) => {
        expect(err.message).to.equal('Argument must be an array of CIDs')
        expect(result).to.be.undefined()
        done()
      })
    })
  })
})
