/* eslint-env mocha */
'use strict'

/*
 * Test different types of data structures living together
 * &
 * Test data made of mixed data structures!
 */

const chai = require('chai')
const chaiAsProised = require('chai-as-promised')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(chaiAsProised)
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

  describe('get', () => {
    it('should return nodes correctly', async () => {
      const result = resolver.get([cidCbor, cidPb])
      const node1 = await result.next().value
      expect(node1).to.eql(nodeCbor)

      const node2 = await result.next().value
      expect(node2).to.eql(nodePb)
    })

    it('should return nodes in input order', async () => {
      const result = resolver.get([cidPb, cidCbor])
      const node1 = await result.next().value
      expect(node1).to.eql(nodePb)

      const node2 = await result.next().value
      expect(node2).to.eql(nodeCbor)
    })

    it('should return error on invalid CID', async () => {
      const result = resolver.get([cidCbor, 'invalidcid'])
      // TODO vmx 2018-12-11: This should really fail on the second node
      // we get, as the first one is valid. This is only possible once
      // the `getmany()` call of the BlockService takes and returns an
      // iterator and not an array.
      await expect(result.next().value).to.be.rejectedWith(
        'Not a valid cid')
    })

    it('should return error on non-existent CID', async () => {
      const nonExistentCid = new CID(
        'Qma4hjFTnCasJ8PVp3mZbZK5g2vGDT4LByLJ7m8ciyRFZP')
      const result = resolver.get([cidCbor, nonExistentCid])
      // TODO vmx 2018-12-11: This should really fail on the second node
      // we get, as the first one is valid. This is only possible once
      // the `getmany()` call of the BlockService takes and returns an
      // iterator and not an array.
      await expect(result.next().value).to.be.rejectedWith(
        'Not Found')
    })

    it('should return error on invalid input', () => {
      expect(() => resolver.get('astring')).to.throw(
        '`cids` must be an iterable of CIDs')
    })
  })
})
