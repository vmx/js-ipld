/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(dirtyChai)
const rlp = require('rlp')
const BlockService = require('ipfs-block-service')
const ipldEthBlock = require('ipld-ethereum').ethBlock
const ipldEthStateTrie = require('ipld-ethereum').ethStateTrie
const loadFixture = require('aegir/fixtures')
const async = require('async')
const EthBlockHeader = require('ethereumjs-block/header')
const EthTrieNode = require('merkle-patricia-tree/trieNode')
const multicodec = require('multicodec')

const IPLDResolver = require('../src')

module.exports = (repo) => {
  describe('IPLD Resolver with eth-* (resolving across various Eth objects)', () => {
    let resolver

    let ethObjs

    before(function (done) {
      this.timeout(10 * 1000)
      const bs = new BlockService(repo)
      resolver = new IPLDResolver({
        blockService: bs,
        formats: [ipldEthBlock, ipldEthStateTrie]
      })

      async.waterfall([
        readFilesFixture,
        generateCids
      ], done)

      function readFilesFixture (cb) {
        async.parallel({
          child: (cb) => cb(null, loadFixture('test/fixtures/block_302517')),
          block: (cb) => cb(null, loadFixture('test/fixtures/block_302516')),
          stateRoot: (cb) => cb(null, loadFixture('test/fixtures/state_r_302516')),
          state0: (cb) => cb(null, loadFixture('test/fixtures/state_0_302516')),
          state00: (cb) => cb(null, loadFixture('test/fixtures/state_00_302516')),
          state000: (cb) => cb(null, loadFixture('test/fixtures/state_000_302516')),
          state0000: (cb) => cb(null, loadFixture('test/fixtures/state_0000_302516')),
          state00001: (cb) => cb(null, loadFixture('test/fixtures/state_00001_302516')),
          state000017: (cb) => cb(null, loadFixture('test/fixtures/state_000017_302516'))
        }, cb)
      }

      function generateCids (fileData, cb) {
        ethObjs = {
          child: generateForType('child', 'eth-block', fileData.child),
          block: generateForType('block', 'eth-block', fileData.block),
          stateRoot: generateForType('stateRoot', 'eth-state-trie', fileData.stateRoot),
          state0: generateForType('state0', 'eth-state-trie', fileData.state0),
          state00: generateForType('state00', 'eth-state-trie', fileData.state00),
          state000: generateForType('state000', 'eth-state-trie', fileData.state000),
          state0000: generateForType('state0000', 'eth-state-trie', fileData.state0000),
          state00001: generateForType('state00001', 'eth-state-trie', fileData.state00001),
          state000017: generateForType('state000017', 'eth-state-trie', fileData.state000017)
        }

        cb()
      }

      async function generateForType (label, type, rawData) {
        let node

        switch (type) {
          case 'eth-block': node = new EthBlockHeader(rawData); break
          case 'eth-state-trie': node = new EthTrieNode(rlp.decode(rawData)); break
          default: throw new Error('Unknown type!')
        }

        // TODO vmx 2018-12-07: Make multicodec use constants
        const format = multicodec.getCodeVarint(type).readUInt16BE(0)
        const result = resolver.put([node], { format: format })
        const cid = await result.first()

        return {
          raw: rawData,
          node: node,
          cid: cid
        }
      }
    })

    describe('resolver.get', () => {
      it('block-to-block', async () => {
        const child = await ethObjs.child
        const result = resolver.resolve(child.cid, 'parent')

        const node1 = await result.next().value
        expect(node1.remainderPath).to.eql('')

        const node2 = await result.next().value
        expect(node2.remainderPath).to.eql('')
        expect(node2.value.number.toString('hex')).to.eql('302516')
      })

      it('block-to-account resolve', async () => {
        const child = await ethObjs.child
        const result = resolver.resolve(child.cid,
          'parent/state/0/0/0/0/1/7/2/7/8/a/1/e/6/e/9/6/3/5/e/1/a/3/f/1/1/e/b/0/2/2/d/a/1/f/5/7/e/a/0/0/4/d/8/5/2/d/9/d/1/9/4/2/d/4/3/6/0/8/5/4/0/4/7/1/nonce')
        const node = await result.last()
        expect(node.value.toString('hex'), '03')
        expect(node.remainderPath).to.equal('')
      })
    })
  })
}
