/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const chaiAsProised = require('chai-as-promised')
const expect = chai.expect
chai.use(dirtyChai)
chai.use(chaiAsProised)
const BlockService = require('ipfs-block-service')
const dagCBOR = require('ipld-dag-cbor')
const series = require('async/series')
const each = require('async/each')
const multicodec = require('multicodec')
const multihash = require('multihashes')

const IPLDResolver = require('../src')

module.exports = (repo) => {
  describe('IPLD Resolver with dag-cbor (MerkleDAG CBOR)', () => {
    let resolver

    let node1
    let node2
    let node3
    let cid1
    let cid2
    let cid3

    before((done) => {
      const bs = new BlockService(repo)

      resolver = new IPLDResolver({ blockService: bs })

      series([
        (cb) => {
          node1 = { someData: 'I am 1' }

          dagCBOR.util.cid(node1, (err, cid) => {
            expect(err).to.not.exist()
            cid1 = cid
            cb()
          })
        },
        (cb) => {
          node2 = {
            someData: 'I am 2',
            one: cid1
          }

          dagCBOR.util.cid(node2, (err, cid) => {
            expect(err).to.not.exist()
            cid2 = cid
            cb()
          })
        },
        (cb) => {
          node3 = {
            someData: 'I am 3',
            one: cid1,
            two: cid2
          }

          dagCBOR.util.cid(node3, (err, cid) => {
            expect(err).to.not.exist()
            cid3 = cid
            cb()
          })
        }
      ], store)

      async function store () {
        const nodes = [node1, node2, node3]
        const result = resolver.put(nodes, { format: multicodec.DAG_CBOR })
        cid1 = await result.first()
        cid2 = await result.first()
        cid3 = await result.first()

        done()
      }
    })

    describe('internals', () => {
      it('resolver._put', (done) => {
        each([
          { node: node1, cid: cid1 },
          { node: node2, cid: cid2 },
          { node: node3, cid: cid3 }
        ], (nc, cb) => {
          resolver._put(nc.cid, nc.node, cb)
        }, done)
      })
    })

    describe('public api', () => {
      it('resolver.put with format', async () => {
        const result = resolver.put([node1], { format: multicodec.DAG_CBOR })
        const cid = await result.first()
        expect(cid.version).to.equal(1)
        expect(cid.codec).to.equal(multicodec.DAG_CBOR)
        expect(cid.multihash).to.exist()
        const mh = multihash.decode(cid.multihash)
        expect(mh.name).to.equal('sha2-256')
      })

      it('resolver.put with format + hashAlg', async () => {
        const result = resolver.put([node1], {
          format: multicodec.DAG_CBOR,
          hashAlg: multicodec.SHA3_512
        })
        const cid = await result.first()
        expect(cid).to.exist()
        expect(cid.version).to.equal(1)
        expect(cid.codec).to.equal(multicodec.DAG_CBOR)
        expect(cid.multihash).to.exist()
        const mh = multihash.decode(cid.multihash)
        expect(mh.name).to.equal('sha3-512')
      })

      // TODO vmx 2018-11-30: Implement getting the whole object properly
      // it('resolver.get root path', (done) => {
      //   resolver.get(cid1, '/', (err, result) => {
      //     expect(err).to.not.exist()
      //
      //     dagCBOR.util.cid(result.value, (err, cid) => {
      //       expect(err).to.not.exist()
      //       expect(cid).to.eql(cid1)
      //       done()
      //     })
      //   })
      // })

      it('resolver.get value within 1st node scope', async () => {
        const result = resolver.resolve(cid1, 'someData')
        const node = await result.first()
        expect(node.remainderPath).to.eql('')
        expect(node.value).to.eql('I am 1')
      })

      it('resolver.get value within nested scope (0 level)', async () => {
        const result = resolver.resolve(cid2, 'one')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('')
        expect(node1.value).to.eql(cid1)

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('')
        expect(node2.value).to.eql({ someData: 'I am 1' })
      })

      it('resolver.get value within nested scope (1 level)', async () => {
        const result = resolver.resolve(cid2, 'one/someData')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('someData')
        expect(node1.value).to.eql(cid1)

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('')
        expect(node2.value).to.eql('I am 1')
      })

      it('resolver.get value within nested scope (2 levels)', async () => {
        const result = resolver.resolve(cid3, 'two/one/someData')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('one/someData')
        expect(node1.value).to.eql(cid2)

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('someData')
        expect(node2.value).to.eql(cid1)

        const node3 = await result.first()
        expect(node3.remainderPath).to.eql('')
        expect(node3.value).to.eql('I am 1')
      })

      it('resolver.get calls callback for unavailable path', async () => {
        const result = resolver.resolve(cid3, `foo/${Date.now()}`)
        await expect(result.next()).to.be.rejectedWith(
          'path not available at root')
      })

      it('resolver.get round-trip', async () => {
        const resultPut = resolver.put([node1], {
          format: multicodec.DAG_CBOR
        })
        const cid = await resultPut.first()
        const resultGet = resolver.get([cid])
        const node = await resultGet.first()
        expect(node).to.deep.equal(node1)
      })

      it('resolver.remove', async () => {
        const resultPut = resolver.put([node1], {
          format: multicodec.DAG_CBOR
        })
        const cid = await resultPut.first()
        const resultGet = resolver.get([cid])
        const sameAsNode1 = await resultGet.first()
        expect(sameAsNode1).to.deep.equal(node1)
        return remove()

        async function remove () {
          const resultRemove = resolver.remove([cid])
          // The items are deleted through iteration
          await resultRemove.last()
          // Verify that the item got really deleted
          const resultGet = resolver.get([cid])
          await expect(resultGet.next()).to.eventually.be.rejected()
        }
      })
    })
  })
}
