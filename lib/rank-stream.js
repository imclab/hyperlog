var from = require('from2')
var mutexify = require('mutexify')

var noop = function() {}

var indexOf = function(list, rank){
  var low = 0
  var high = list.length
  var mid = 0

  while (low < high) {
    mid = (low + high) >> 1
    if (rank < list[mid].rank) high = mid
    else low = mid + 1
  }

  return low
}

var rankStream = function(logs) {
  var cache = []
  var lock = mutexify()
  var missing = {}

  var notify = null
  var current = null

  var rs =  from.obj(function(size, cb) {
    lock(function(release) {
      var node = cache.shift() || null
      if (node) push(node.log, node.seq+1)
      release(cb, null, node)
    })
  })

  var push = function(name, seq, cb) {
    lock(function run(release) {
      if (!missing[name]) return release(cb)
      logs.get(name, seq, function(err, node) {
        if (err && !err.notFound) return release(cb, err)

        if (err) {
          current = name
          notify = run.bind(null, release)
          return
        }

        if (!--missing[name]) delete missing[name]

        cache.splice(indexOf(cache, node.rank), 0, node)
        release(cb)
      })
    })
  }

  rs.add = function(name, from, to, cb) {
    if (!cb) cb = noop
    missing[name] = to - from + 1
    push(name, from, cb)
  }

  rs.update = function(name) {
    if (current !== name) return
    var fn = notify
    notify = null
    if (fn) fn()
  }

  return rs
}

module.exports = rankStream

if (require.main !== module) return

var db = require('memdb')()
var logs = require('./logs')('tmp', db)

var ranks = module.exports(logs)

ranks.add('john', 1, 3)
ranks.add('maf', 1, 2)

ranks.on('data', function(data) {
  console.log(data)
})

ranks.on('end', function() {
  console.log('(no more data)')
})

db.put(logs.key('john', 1), logs.value({hash:'test', log:'john', seq:1, rank:1, value:new Buffer('whatevs')}), function() {
  ranks.update('john')
  db.put(logs.key('john', 2), logs.value({hash:'test', log:'john', seq:2, rank:3, value:new Buffer('NEXT')}), function() {
    ranks.update('john')
    db.put(logs.key('maf', 1), logs.value({hash:'test', log:'maf', seq:1, rank:2, value:new Buffer('NEXT')}), function() {
      ranks.update('maf')
      db.put(logs.key('maf', 2), logs.value({hash:'test', log:'maf', seq:2, rank:10, value:new Buffer('NEXT')}), function() {
        ranks.update('maf')
        db.put(logs.key('john', 3), logs.value({hash:'test', log:'john', seq:3, rank:4, value:new Buffer('NEXT')}), function() {
          ranks.update('john')
        })
      })
    })
  })
})