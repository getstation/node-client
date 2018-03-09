var errors = require('./errors');

var EventSource = require('./eventsource');

function StreamProcessor(sdk_key, config, requestor) {
  var processor = {},
      store = config.feature_store,
      es;

  processor.start = function(fn) {
    var cb = fn || function(){};
    es = new EventSource(config.stream_uri + "/flags", 
      {
        agent: config.proxy_agent, 
        headers: {'Authorization': sdk_key,'User-Agent': config.user_agent}
      });
      
    es.onerror = function(err) {
      cb(new errors.LDStreamingError(err.message, err.code));
    };

    es.addEventListener('put', function(e) {
      config.logger.debug('Received put event');
      if (e && e.data) {
        try {
          var flags = JSON.parse(e.data);
          store.init(flags, function() {
            cb();
          })
        } catch (err) {
          cb(new errors.LDStreamingError(`[put] Data ${e.data}\n${err.message}`, err.code));
        }
      } else {
        cb(new errors.LDStreamingError('Unexpected payload from event stream'));
      }
    });

    es.addEventListener('patch', function(e) {
      config.logger.debug('Received patch event');
      if (e && e.data) {
        try {
          var patch = JSON.parse(e.data);
          store.upsert(patch.data.key, patch.data);
        } catch (err) {
          cb(new errors.LDStreamingError(`[patch] Data ${e.data}\n${err.message}`, err.code));
        }
      } else {
        cb(new errors.LDStreamingError('Unexpected payload from event stream'));
      }
    });

    es.addEventListener('delete', function(e) {
      config.logger.debug('Received delete event');
      if (e && e.data) {
        try {
          var data = JSON.parse(e.data),
              key = data.path.charAt(0) === '/' ? data.path.substring(1) : data.path, // trim leading '/'
              version = data.version;

          store.delete(key, version);
        } catch (err) {
          cb(new errors.LDStreamingError(`[delete] Data ${e.data}\n${err.message}`, err.code));
        }
      } else {
        cb(new errors.LDStreamingError('Unexpected payload from event stream'));
      }
    });

    es.addEventListener('indirect/put', function(e) {
      config.logger.debug('Received indirect put event')
      requestor.request_all_flags(function (err, flags) {
        if (err) {
          cb(err);
        } else {
          try {
            store.init(JSON.parse(flags), function() {
              cb();
            })
          } catch (err) {
            cb(new errors.LDStreamingError(`[indirect/put] Data ${e.data}\n${err.message}`, err.code));
          }
        }
      })
    });

    es.addEventListener('indirect/patch', function(e) {
      config.logger.debug('Received indirect patch event')
      if (e && e.data) {
        var key = e.data.charAt(0) === '/' ? e.data.substring(1) : e.data;
        requestor.request_flag(key, function(err, flag) {
          if (err) {
            cb(new errors.LDStreamingError('Unexpected error requesting feature flag'));
          } else {
            try {
              store.upsert(key, JSON.parse(flag));
            } catch (err) {
              cb(new errors.LDStreamingError(`[indirect/patch] Data ${e.data}\n${err.message}`, err.code));
            }
          }
        })
      } else {
        cb(new errors.LDStreamingError('Unexpected payload from event stream'));
      }
    });
  }

  processor.stop = function() {
    if (es) {
      es.close();
    }
  }

  processor.close = function() {
    this.stop();
  }


  return processor;
}

module.exports = StreamProcessor;