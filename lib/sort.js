var lexint = require('lexicographic-integer')

var LOG = '!log!'

var noop = function() {}

var sortByRank = function(a, b) {
  return a.rank - b.rank
}

var Sort = function(logs) {
  if (!(this instanceof Sort)) return new Sort(logs)
  this.logs = logs
  this.cache = []
}

Sort.prototype.shift = function(cb) {
  var self = this
  var first = this.cache.shift() || null
  if (!first) return cb(null, first)
  
  this.logs.get(first.log, first.seq+1, function(err, node) {
    if (err && err.notFound) return cb(null, first)
    if (err) return cb(err)
    self.cache.push(node)
    self.cache.sort(sortByRank)
    cb(null, first)
  })
}

Sort.prototype.push = function(log, from, cb) {
  if (!cb) cb = noop

  var self = this

  this.logs.get(log, from, function(err, node) {
    if (err) return cb(err)
    self.cache.push(node)
    self.cache.sort(sortByRank)
    cb()
  })
}

module.exports = Sort
