/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(dirtyChai)
const BlockService = require('ipfs-block-service')
const ipldEthBlock = require('ipld-ethereum').ethBlock
const EthBlockHeader = require('ethereumjs-block/header')
const multihash = require('multihashes')
const each = require('async/each')
const multicodec = require('multicodec')

const IPLDResolver = require('../src')

module.exports = (repo) => {
  describe('IPLD Resolver with eth-block (Ethereum Block)', () => {
    let resolver

    let node1
    let node2
    let node3
    let cid1
    let cid2
    let cid3

    // TODO vmx 2018-12-07: Make multicodec use constants
    const formatEthBlock = multicodec.getCodeVarint('eth-block')
      .readUInt16BE(0)

    before(async () => {
      const bs = new BlockService(repo)
      resolver = new IPLDResolver({
        blockService: bs,
        formats: [ipldEthBlock]
      })

      node1 = new EthBlockHeader({
        number: 1
      })
      node2 = new EthBlockHeader({
        number: 2,
        parentHash: node1.hash()
      })
      node3 = new EthBlockHeader({
        number: 3,
        parentHash: node2.hash()
      })

      const nodes = [node1, node2, node3]
      const result = resolver.put(nodes, { format: formatEthBlock })
      cid1 = await result.first()
      cid2 = await result.first()
      cid3 = await result.first()
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
        const result = resolver.put([node1], { format: formatEthBlock })
        const cid = await result.first()
        expect(cid.version).to.equal(1)
        expect(cid.codec).to.equal('eth-block')
        expect(cid.multihash).to.exist()
        const mh = multihash.decode(cid.multihash)
        expect(mh.name).to.equal('keccak-256')
      })

      it('resolver.put with format + hashAlg', async () => {
        // TODO vmx 2018-12-07: Make multicodec use constants
        const hashAlgKeccak512 = multicodec.getCodeVarint('keccak-512')
          .readUInt8(0)

        const result = resolver.put([node1], {
          format: formatEthBlock,
          hashAlg: hashAlgKeccak512
        })
        const cid = await result.first()
        expect(cid.version).to.equal(1)
        expect(cid.codec).to.equal('eth-block')
        expect(cid.multihash).to.exist()
        const mh = multihash.decode(cid.multihash)
        expect(mh.name).to.equal('keccak-512')
      })

      // TODO vmx 2018-11-30: Implement getting the whole object properly
      // it('root path (same as get)', (done) => {
      //   resolver.get(cid1, '/', (err, result) => {
      //     expect(err).to.not.exist()
      //
      //     ipldEthBlock.util.cid(result.value, (err, cid) => {
      //       expect(err).to.not.exist()
      //       expect(cid).to.eql(cid1)
      //       done()
      //     })
      //   })
      // })

      it('value within 1st node scope', async () => {
        const result = resolver.resolve(cid1, 'number')
        const node = await result.first()
        expect(node.remainderPath).to.eql('')
        expect(node.value.toString('hex')).to.eql('01')
      })

      it('value within nested scope (1 level)', async () => {
        const result = resolver.resolve(cid2, 'parent/number')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('number')
        expect(node1.value).to.eql(cid1)

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('')
        expect(node2.value.toString('hex')).to.eql('01')
      })

      it('value within nested scope (2 levels)', async () => {
        const result = resolver.resolve(cid3, 'parent/parent/number')

        const node1 = await result.first()
        expect(node1.remainderPath).to.eql('parent/number')
        expect(node1.value).to.eql(cid2)

        const node2 = await result.first()
        expect(node2.remainderPath).to.eql('number')
        expect(node2.value).to.eql(cid1)

        const node3 = await result.first()
        expect(node3.remainderPath).to.eql('')
        expect(node3.value.toString('hex')).to.eql('01')
      })

      it('resolver.get round-trip', async () => {
        const resultPut = resolver.put([node1], { format: formatEthBlock })
        const cid = await resultPut.first()
        const resultGet = resolver.get([cid])
        const node = await resultGet.first()
        // TODO vmx 2018-12-12: Find out why the full nodes not deep equal
        expect(node.raw).to.deep.equal(node1.raw)
      })

      it('resolver.remove', async () => {
        const resultPut = resolver.put([node1], { format: formatEthBlock })
        const cid = await resultPut.first()
        const resultGet = resolver.get([cid])
        const sameAsNode1 = await resultGet.first()
        expect(sameAsNode1.raw).to.deep.equal(node1.raw)
        return remove()

        function remove () {
          return new Promise((resolve, reject) => {
            resolver.remove(cid, (err) => {
              expect(err).to.not.exist()
              const resultGet = resolver.get([cid])
              expect(resultGet.next().value).to.eventually.be.rejected()
                // eslint-disable-next-line max-nested-callbacks
                .then(() => resolve())
                // eslint-disable-next-line max-nested-callbacks
                .catch((err) => reject(err))
            })
          })
        }
      })
    })
  })
}
