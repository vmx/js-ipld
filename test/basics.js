/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const chaiAsProised = require('chai-as-promised')
const expect = chai.expect
chai.use(dirtyChai)
chai.use(chaiAsProised)
const BlockService = require('ipfs-block-service')
const CID = require('cids')
const multihash = require('multihashes')
const multicodec = require('multicodec')

const IPLDResolver = require('../src')

module.exports = (repo) => {
  describe('basics', () => {
    it('creates an instance', () => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      expect(r.bs).to.exist()
    })

    it('creates an in memory repo if no blockService is passed', () => {
      IPLDResolver.inMemory((err, r) => {
        expect(err).to.not.exist()
        expect(r.bs).to.exist()
      })
    })

    it.skip('add support to a new format', () => {})
    it.skip('remove support to a new format', () => {})
  })

  describe('validation', () => {
    it('resolve - errors on unknown resolver', async () => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      // choosing a format that is not supported
      const cid = new CID(
        1,
        multicodec.BLAKE2B_8,
        multihash.encode(Buffer.from('abcd', 'hex'), 'sha1')
      )
      const result = r.resolve(cid, '')
      await expect(result.next()).to.be.rejectedWith(
        'No resolver found for codec "blake2b-8"')
    })

    it('put - errors on unknown resolver', async () => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      // choosing a format that is not supported
      const result = r.put([null], { format: multicodec.BLAKE2B_8 })
      await expect(result.next()).to.be.rejectedWith(
        'No resolver found for codec "blake2b-8"')
    })

    it('put - errors if no options', () => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      expect(() => r.put([null])).to.be.throw('`put` requires options')
    })

    it('_put - errors on unknown resolver', (done) => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      // choosing a format that is not supported
      const cid = new CID(
        1,
        multicodec.BLAKE2B_8,
        multihash.encode(Buffer.from('abcd', 'hex'), 'sha1')
      )
      r._put(cid, null, (err, result) => {
        expect(err).to.exist()
        expect(err.message).to.eql('No resolver found for codec "blake2b-8"')
        done()
      })
    })
  })
}
