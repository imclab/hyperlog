var after = require('after-all')
var mutexify = require('mutexify')
var crypto = require('crypto')
var cuid = require('cuid')
var events = require('events')
var util = require('util')
var logs = require('./lib/logs')

var NODE = 'node!'
var ID = 'id!'

var Hyperlog = function(db, opts) {
  if (!(this instanceof Hyperlog)) return new Hyperlog(db, opts)
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  this.db = db
  this.id = opts.id
  this.logs = logs('logs', db)
  this.lock = mutexify()

  var self = this
  this.lock(function(release) {
    var done = function(err) {
      if (err) return self.emit('error', err)
      self.emit('ready')
      release()
    }

    db.get(ID, {valueEncoding:'utf-8'}, function(err, id) {
      if (err && !err.notFound) return done(err)
      self.id = id || opts.id || cuid()
      if (self.id === id) return done()
      db.put(ID, self.id, done)
    })
  })
}

util.inherits(Hyperlog, events.EventEmitter)

Hyperlog.prototype.get = function(hash, cb) {
  var self = this
  this.db.get(NODE+hash, {valueEncoding:'utf-8'}, function(err, ref) {
    if (err) return cb(err)
    var i = ref.indexOf('!')
    self.logs.get(ref.slice(0, i), parseInt(ref.slice(i+1)), cb)
  })  
}

var hash = function(value, links) {
  var sha = crypto.createHash('sha1')

  sha.update(''+value.length+'\n')
  sha.update(value)
  for (var i = 0; i < links.length; i++) sha.update(links[i].hash+'\n')

  return sha.digest('hex')
}

var ensure = function(self, node, cb) {
  self.db.get(NODE+node.hash, {valueEncoding:'utf-8'}, function(err) {
    if (err) return cb(err)
    cb(null, node)
  })
}

var add = function(self, log, links, value, cb) {
  var self = self

  var node = {
    hash: null,
    log: log,
    seq: 0,
    rank: 0,
    links: links,
    value: value
  }

  var next = after(function(err) {
    if (err) return cb(err)

    node.hash = hash(value, links)

    var batch = []

    batch.push({
      type: 'put',
      key: self.logs.key(log, node.seq),
      value: self.logs.value(node)
    })

    batch.push({
      type: 'put',
      key: NODE+node.hash,
      value: node.log+'!'+node.seq
    })

    self.get(node.hash, function(err, oldNode) {
      if (!err) return cb(err, oldNode)
      self.db.batch(batch, function(err) {
        if (err) return cb(err)
        cb(null, node)
      })
    })
  })

  links.forEach(function(link, i) {
    var n = next()

    var onlink = function(err, resolved) {
      if (err) return n(err)
      node.rank = Math.max(node.rank, resolved.rank+1)
      links[i] = resolved
      n()      
    }

    if (typeof link === 'string') self.get(link, onlink)
    else ensure(self, link, onlink)
  })

  var n = next()
  self.logs.tail(log, function(err, seq) {
    if (err) return n(err)
    node.seq = 1+seq

    if (!seq) {
      node.rank = Math.max(node.rank, 1)
      return n()
    }

    self.logs.get(log, seq, function(err, prev) {
      if (err) return n(err)
      node.rank = Math.max(node.rank, prev.rank+1)
      n()
    })
  })
}

Hyperlog.prototype.add = function(links, value, opts, cb) {
  if (typeof opts === 'function') return this.add(links, value, null, opts)
  if (!links) links = []
  if (!Array.isArray(links)) links = [links]
  if (!Buffer.isBuffer(value)) value = new Buffer(value)
  if (!cb) cb = noop

  var self = this

  this.lock(function(release) {
    add(self, opts && opts.log || self.id, links, value, function(err, node) {
      if (err) return release(cb, err)
      release(cb, null, node)
    })
  })
}

module.exports = Hyperlog