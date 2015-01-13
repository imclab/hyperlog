var lexint = require('lexicographic-integer')

var LOG = '!log!'

var noop = function() {}

var sortByOrder = function(a, b) {
  return a.order - b.order
}

var parseSeq = function(key) {
  return lexint.unpack(key.slice(key.lastIndexOf('!')+1), 'hex')  
}

var Sort = function(db) {
  if (!(this instanceof Sort)) return new Sort(db)
  this.db = db
  this.cache = []
}

Sort.prototype.shift = function(cb) {
  var self = this
  var first = this.cache.shift() || null
  if (!first) return cb(null, first)

  var seq = parseSeq(first.key)
  var log = first.key.slice(LOG.length, first.key.indexOf('!', LOG.length))
  var key = LOG+log+'!'+lexint.pack(seq+1, 'hex')
  
  this.db.get(key, function(err, value) {
    if (err && err.notFound) return cb(null, first)
    if (err) return cb(err)
    self.cache.push({order:parseSeq(key), key:key, value:value})
    self.cache.sort(sortByOrder)
    cb(null, first)
  })
}

Sort.prototype.add = function(log, from, cb) {
  if (!cb) cb = noop

  var self = this
  var key = LOG+log+'!'+lexint.pack(from, 'hex')

  self.db.get(key, function(err, value) {
    if (err) return cb(err)
    self.cache.push({order:parseSeq(key), key:key, value:value})
    self.cache.sort(sortByOrder)
    cb()
  })
}

module.exports = Sort

if (require.main !== module) return

var level = require('level')
var db = level('db')
var s = Sort(db)

var done = function() {
  s.shift(function(err, val) {
    if (err) throw err
    console.log(val)
    if (val) done()
  })
}

s.add('a', 0, function() {
  s.add('b', 20, function() {
    s.add('c', 10, function() {
      s.add('d', 25, function() {
        s.add('e', 5, function() {
          s.add('hello', 0, done)  
        })  
      })  
    })
  })
})
