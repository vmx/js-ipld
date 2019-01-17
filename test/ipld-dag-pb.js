/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(dirtyChai)
const BlockService = require('ipfs-block-service')
const dagPB = require('ipld-dag-pb')
const series = require('async/series')
const each = require('async/each')
const multihash = require('multihashes')
const multicodec = require('multicodec')
const IPLDResolver = require('../src')

module.exports = (repo) => {
  describe('IPLD Resolver with dag-pb (MerkleDAG Protobuf)', () => {
    let resolver
    let node1
    let node2
    let node3
    let cid1
    let cid2
    let cid3

    // TODO vmx 2018-12-07: Make multicodec use constants
    const formatDagPb = multicodec.getCodeVarint('dag-pb').readUInt8(0)

    before((done) => {
      const bs = new BlockService(repo)

      resolver = new IPLDResolver({ blockService: bs })

      series([
        (cb) => {
          dagPB.DAGNode.create(Buffer.from('I am 1'), (err, node) => {
            expect(err).to.not.exist()
            node1 = node
            cb()
          })
        },
        (cb) => {
          dagPB.DAGNode.create(Buffer.from('I am 2'), (err, node) => {
            expect(err).to.not.exist()
            node2 = node
            cb()
          })
        },
        (cb) => {
          dagPB.DAGNode.create(Buffer.from('I am 3'), (err, node) => {
            expect(err).to.not.exist()
            node3 = node
            cb()
          })
        },
        (cb) => {
          dagPB.util.cid(node1, (err, cid) => {
            expect(err).to.not.exist()

            dagPB.DAGNode.addLink(node2, {
              name: '1',
              size: node1.size,
              cid
            }, (err, node) => {
              expect(err).to.not.exist()
              node2 = node
              cb()
            })
          })
        },
        (cb) => {
          dagPB.util.cid(node1, (err, cid) => {
            expect(err).to.not.exist()

            dagPB.DAGNode.addLink(node3, {
              name: '1',
              size: node1.size,
              cid
            }, (err, node) => {
              expect(err).to.not.exist()
              node3 = node
              cb()
            })
          })
        },
        (cb) => {
          dagPB.util.cid(node2, (err, cid) => {
            expect(err).to.not.exist()

            dagPB.DAGNode.addLink(node3, {
              name: '2',
              size: node2.size,
              cid
            }, (err, node) => {
              expect(err).to.not.exist()
              node3 = node
              cb()
            })
          })
        }
      ], store)

      async function store () {
        const nodes = [node1, node2, node3]
        const result = resolver.put(nodes, { format: formatDagPb })
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

      // TODO vmx 2018-11-29 Change this test to use `get()`.
      // it('resolver._get', (done) => {
      //   resolver.put(node1, { cid: cid1 }, (err) => {
      //     expect(err).to.not.exist()
      //     resolver._get(cid1, (err, node) => {
      //       expect(err).to.not.exist()
      //       done()
      //     })
      //   })
      // })
    })

    describe('public api', () => {
      it('resolver.put with format', async () => {
        const result = resolver.put([node1], { format: formatDagPb })
        const cid = await result.first()
        expect(cid.version).to.equal(1)
        expect(cid.codec).to.equal('dag-pb')
        expect(cid.multihash).to.exist()
        const mh = multihash.decode(cid.multihash)
        expect(mh.name).to.equal('sha2-256')
      })

      it('resolver.put with format + hashAlg', async () => {
        // TODO vmx 2018-12-07: Make multicodec use constants
        const hashAlgSha3512 = multicodec.getCodeVarint('sha3-512')
          .readUInt8(0)

        const result = resolver.put([node1], {
          format: formatDagPb, hashAlg: hashAlgSha3512
        })
        const cid = await result.first()
        expect(cid.version).to.equal(1)
        expect(cid.codec).to.equal('dag-pb')
        expect(cid.multihash).to.exist()
        const mh = multihash.decode(cid.multihash)
        expect(mh.name).to.equal('sha3-512')
      })

      // TODO vmx 2018-11-29: Change this test to use the new `get()`
      // it('resolver.get with empty path', (done) => {
      //   resolver.get(cid1, '/', (err, result) => {
      //     expect(err).to.not.exist()
      //
      //     dagPB.util.cid(result.value, (err, cid) => {
      //       expect(err).to.not.exist()
      //       expect(cid).to.eql(cid1)
      //       done()
      //     })
      //   })
      // })

      it('resolves()s a value within 1st node scope', async () => {
        const result = resolver.resolve(cid1, 'Data')
        const node = await result.first()
        expect(node.remainderPath).to.eql('')
        expect(node.value).to.eql(Buffer.from('I am 1'))
      })

      it('resolves()s a value within nested scope (1 level)', async () => {
        const result = resolver.resolve(cid2, 'Links/0/Hash/Data')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('Data')
        expect(node1.value).to.eql(cid1.toV0())

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('')
        expect(node2.value).to.eql(Buffer.from('I am 1'))
      })

      it('resolver.get value within nested scope (2 levels)', async () => {
        const result = resolver.resolve(cid3, 'Links/1/Hash/Links/0/Hash/Data')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('Links/0/Hash/Data')
        expect(node1.value).to.eql(cid2.toV0())

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('Data')
        expect(node2.value).to.eql(cid1.toV0())

        const node3 = await result.first()
        expect(node3.remainderPath).to.eql('')
        expect(node3.value).to.eql(Buffer.from('I am 1'))
      })

      // TODO vmx 3018-11-29: Think about if every returned node should contain
      // a `cid` field or not
      // it('resolver.get value within nested scope (1 level) returns cid of node traversed to', (done) => {
      //   resolver.get(cid2, 'Links/0/Hash/Data', (err, result) => {
      //     expect(err).to.not.exist()
      //     expect(result.cid).to.deep.equal(cid1)
      //     done()
      //   })
      // })

      // TODO vmx 2018-11-29: remove this `get()` call with the new `get()`
      // it('resolver.remove', (done) => {
      //   resolver.put(node1, { cid: cid1 }, (err) => {
      //     expect(err).to.not.exist()
      //     resolver.get(cid1, (err, node) => {
      //       expect(err).to.not.exist()
      //       remove()
      //     })
      //   })
      //
      //   function remove () {
      //     resolver.remove(cid1, (err) => {
      //       expect(err).to.not.exist()
      //       resolver.get(cid1, (err) => {
      //         expect(err).to.exist()
      //         done()
      //       })
      //     })
      //   }
      // })
    })
  })
}
