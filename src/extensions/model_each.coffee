###
  backbone-mongo.js 0.6.0
  Copyright (c) 2013-2014 Vidigami
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
  Source: https://github.com/vidigami/backbone-orm
  Dependencies: Backbone.js and Underscore.js.
###

{_} = require 'backbone-orm'
Cursor = null

module.exports = (cursorFactory, query, iterator, callback) ->

  Cursor = require '../cursor' unless Cursor # module dependencies

  options = query.$each or {}

  # method = if options.json then 'toJSON' else 'toModels'

  processed_count = 0
  parsed_query = Cursor.parseQuery(_.omit(query, '$each'))
  _.defaults(parsed_query.cursor, {$offset: 0, $sort: 'id'})

  model_limit = parsed_query.cursor.$limit or Infinity
  # parsed_query.cursor.$limit = options.fetch if options.fetch

  cursor = cursorFactory(parsed_query)
  cursor._queryToMongoCursor (err, mongo_cursor) ->
    return callback(err) if err
    aggregate_cursor = mongo_cursor.aggregate_cursor
    unless aggregate_cursor
      mongo_cursor.batchSize(options.fetch) if options.fetch

    running_threads = 0
    callback = _.once(callback)
    done = false

    iteratorCallback = (err) ->
      return if callback.ran
      return (callback(err); callback.ran = true) if err
      processed_count++
      running_threads--
      if done or processed_count >= model_limit
        if running_threads <= 0
          callback(null, processed_count)
          callback.ran = true
          return
      else
        processMoreObjects()

    processMoreObjects = ->
      return if running_threads >= options.threads
      running_threads++
      if aggregate_cursor
        aggregate_cursor.next handleAggregateNext
      else
        mongo_cursor.nextObject handleNextItem

    handleAggregateNext = (err, result) ->
      return iteratorCallback(err) if err
      result.id = result.__id.toString()
      delete result._id
      delete result.__id
      handleNextItem(err, result)

    handleNextItem = (err, item) ->
      return iteratorCallback(new Error("Failed to get models. Error: #{err}")) if err
      unless item
        done = true
        return iteratorCallback()
      if options.json
        call_iterator = iterator
      else
        call_iterator = (json, callback) ->
          cursor._createModelsForJSON json, (err, model) ->
            return callback(err) if err
            iterator(model, callback)

      call_iterator(item, iteratorCallback)
      processMoreObjects()

    processMoreObjects()
