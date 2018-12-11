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
const pull = require('pull-stream')

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
      const cid = new CID(1, 'base1', multihash.encode(Buffer.from('abcd', 'hex'), 'sha1'))
      const result = r.resolve(cid, '')
      await expect(result.next().value).to.be.rejectedWith(
        'No resolver found for codec "base1"')
    })

    // TODO vmx 2018-11-29 Change this test to use `get()`.
    // it('_get - errors on unknown resolver', (done) => {
    //   const bs = new BlockService(repo)
    //   const r = new IPLDResolver({ blockService: bs })
    //   // choosing a format that is not supported
    //   const cid = new CID(1, 'base1', multihash.encode(Buffer.from('abcd', 'hex'), 'sha1'))
    //   r.get(cid, (err, result) => {
    //     expect(err).to.exist()
    //     expect(err.message).to.eql('No resolver found for codec "base1"')
    //     done()
    //   })
    // }

    it('put - errors on unknown resolver', (done) => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      // choosing a format that is not supported
      r.put(null, { format: 'base1' }, (err, result) => {
        expect(err).to.exist()
        expect(err.message).to.eql('No resolver found for codec "base1"')
        done()
      })
    })

    it('put - errors if no options', (done) => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      r.put(null, (err, result) => {
        expect(err).to.exist()
        expect(err.message).to.eql('IPLDResolver.put requires options')
        done()
      })
    })

    it('_put - errors on unknown resolver', (done) => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      // choosing a format that is not supported
      const cid = new CID(1, 'base1', multihash.encode(Buffer.from('abcd', 'hex'), 'sha1'))
      r._put(cid, null, (err, result) => {
        expect(err).to.exist()
        expect(err.message).to.eql('No resolver found for codec "base1"')
        done()
      })
    })

    it('treeStream - errors on unknown resolver', (done) => {
      const bs = new BlockService(repo)
      const r = new IPLDResolver({ blockService: bs })
      // choosing a format that is not supported
      const cid = new CID(1, 'base1', multihash.encode(Buffer.from('abcd', 'hex'), 'sha1'))
      pull(
        r.treeStream(cid, '/', {}),
        pull.collect(function (err) {
          expect(err).to.exist()
          expect(err.message).to.eql('No resolver found for codec "base1"')
          done()
        })
      )
    })
  })
}
