var protocol = require('./protocol')
var resolver = require('./resolver')
var through = require('through2')
var pump = require('pump')
var after = require('after-all')

var heads = function(hyper, each, cb) {
  var prev = null
  var filter = function(head, enc, cb) {
    if (prev === head.log) return cb()
    prev = head.log
    each(head, cb)
  }

  return pump(hyper.heads({reverse:true}), through.obj(filter), cb)
}

module.exports = function(hyper, id) {
  var stream = protocol()
  var rcvd = resolver(hyper.tmp)
  var remoteFinalized = false
  var localFinalized = false

  var addMissing = function(hyper, node, cb) {
    var next = after(function(err) {
      if (err) return cb(err)
      cb()
    })

    node.links.forEach(function(link) {
      var cb = next()
      hyper.logs.tail(link.log, function(err, seq) {
        if (err) return cb(err)

        if (seq >= link.seq || link.log === node.log) return cb()
        rcvd.want(link.log, seq+1, link.seq, function(err, inserted) {
          if (err) return cb(err)

          if (inserted) stream.want({log:link.log, seq:seq}, cb)
          else cb()
        })
      })
    })
  }

  var onnode = function(node, enc, cb) {
    stream.node(node, cb)
  }

  var onhave = function(head, cb) {
    stream.have(head, cb)
  }

  var onfin = function() {
    localFinalized = true
    stream.finalize()
    if (remoteFinalized) stream.end()
  }

  stream.on('finalize', function(cb) {
    remoteFinalized = true
    if (localFinalized) stream.end()
    cb()
  })

  stream.on('have', function(have, cb) {
    hyper.logs.tail(have.log, function(err, seq) {
      if (err) return cb(err)
      if (seq >= have.seq) return cb()

      rcvd.want(have.log, seq+1, have.seq, function(err, inserted) {
        if (err) return cb(err)

        if (inserted) stream.want({log:have.log, seq:seq}, cb)
        else cb()
      })
    })
  })

  stream.on('node', function(node, cb) {
    addMissing(hyper, node, function(err) {
      if (err) return cb(err)
      rcvd.push(node, cb)
    })
  })

  stream.on('want', function(want, cb) {
    pump(hyper.logs.createReadStream(want.log, {since:want.seq}), through.obj(onnode))
    cb()
  })

  stream.on('handshake', function(handshake, cb) {
    if (handshake.version !== 1) return cb(new Error('Protocol version not supported'))
    cb()
  })

  stream.on('flush', function(cb) {
    rcvd.shift(function loop(err, node) {
      if (err) return stream.destroy(err)
      if (!node) return onfin()

      console.log('[%s] rcvd: %s %d (%d)', id, node.log, node.seq, node.sort)

      hyper.add(node.links, node.value, {log:node.log}, function(err) {
        if (err) return stream.destroy(err)
        rcvd.shift(loop)
      })
    })

    cb()
  })

  stream.handshake({version:1})
  heads(hyper, onhave, function(err) {
    if (err) return stream.destroy(err)
    stream.flush()
  })

  return stream
}