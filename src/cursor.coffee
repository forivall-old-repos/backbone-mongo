###
  backbone-mongo.js 0.6.3
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-mongo
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
###

{_, sync} = require 'backbone-orm'

ARRAY_QUERIES = ['$or', '$nor', '$and']

_sortArgsToMongo = (args, backbone_adapter) ->
  args = if _.isArray(args) then args else [args]
  sorters = {}
  for sort_part in args
    key = sort_part.trim(); value = 1
    (key = key.substring(1).trim(); value = -1) if key[0] is '-'
    sorters[if key is 'id' then backbone_adapter.id_attribute else key] = value
  return sorters

_adaptIds = (query, backbone_adapter, is_id) ->
  return query if _.isDate(query) or _.isRegExp(query)
  return (_adaptIds(value, backbone_adapter, is_id) for value in query) if _.isArray(query)
  if _.isObject(query)
    result = {}
    for key, value of query
      result[if key is 'id' then backbone_adapter.id_attribute else key] = _adaptIds(value, backbone_adapter, (is_id or key is 'id'))
    return result
  return backbone_adapter.findId(query) if is_id
  return query

module.exports = class MongoCursor extends sync.Cursor
  ##############################################
  # Execution of the Query
  ##############################################
  _queryToMongoCursor: (callback) ->
    @buildFindQuery (err, find_query) =>
      return callback(err) if err

      mongo_query = _adaptIds(find_query, @backbone_adapter)
      mongo_query[@backbone_adapter.id_attribute] = {$in: _adaptIds(@_cursor.$ids, @backbone_adapter, true)} if @_cursor.$ids
      mongo_query[key] = _adaptIds(@_cursor[key], @backbone_adapter) for key in ARRAY_QUERIES when @_cursor[key]

      # only select specific fields
      if @_cursor.$values
        $fields = if @_cursor.$white_list then _.intersection(@_cursor.$values, @_cursor.$white_list) else @_cursor.$values
      else if @_cursor.$select
        $fields = if @_cursor.$white_list then _.intersection(@_cursor.$select, @_cursor.$white_list) else @_cursor.$select
      else if @_cursor.$white_list
        $fields = @_cursor.$white_list

      return @_aggregateCursor(mongo_query, $fields, callback) if @_cursor.$unique

      args = [mongo_query]
      args.push($fields) if $fields
      # add callback and call
      args.push (err, cursor) =>
        return callback(err) if err
        if @_cursor.$sort
          @_cursor.$sort = [@_cursor.$sort] unless _.isArray(@_cursor.$sort)
          cursor = cursor.sort(_sortArgsToMongo(@_cursor.$sort, @backbone_adapter))

        cursor = cursor.skip(@_cursor.$offset) if @_cursor.$offset

        if @_cursor.$one or @hasCursorQuery('$exists')
          cursor = cursor.limit(1)
        else if @_cursor.$limit
          cursor = cursor.limit(@_cursor.$limit)

        callback(null, cursor)

      @connection.collection (err, collection) =>
        return callback(err) if err
        collection.find.apply(collection, args)

  queryToJSON: (callback) ->
    return callback(null, if @hasCursorQuery('$one') then null else []) if @hasCursorQuery('$zero')
    @_queryToMongoCursor (err, cursor) =>
      return callback(err) if err

      return @_aggregateMongoCursorToJSON(aggregate_cursor, callback) if (aggregate_cursor = cursor.aggregate_cursor)

      return cursor.count(callback) if @hasCursorQuery('$count') # only the count
      return cursor.count((err, count) -> callback(err, !!count)) if @hasCursorQuery('$exists') # only if exists

      cursor.toArray (err, docs) =>
        return callback(err) if err
        json = _.map(docs, (doc) => @backbone_adapter.nativeToAttributes(doc))

        @fetchIncludes json, (err) =>
          return callback(err) if err
          if @hasCursorQuery('$page')
            cursor.count (err, count) =>
              return callback(err) if err
              callback(null, {
                offset: @_cursor.$offset or 0
                total_rows: count
                rows: @selectResults(json)
              })
          else
            callback(null, @selectResults(json))

  _aggregateCursor: (match, $fields, callback) =>
    @connection.collection (err, collection) =>
      return callback(err) if err
      pipeline = []
      pipeline.push({$match: match})

      if @_cursor.$sort
        @_cursor.$sort = [@_cursor.$sort] unless _.isArray(@_cursor.$sort)
        sort = {$sort: _sortArgsToMongo(@_cursor.$sort, @backbone_adapter)}
        pipeline.push(sort)

      group_id_args = {}
      (group_id_args[field] = "$#{field}") for field in @_cursor.$unique
      group_args = {_id: group_id_args}

      # Selecting by fields
      # Remove any id fields, they may conflict with the $group _id
      $fields = ($fields or []).concat(@_cursor.$unique)
      $fields = _.without($fields, '_id')
      group_args[field] = {$first: "$#{field}"} for field in $fields
      group_args.__id = {$first: "$#{@backbone_adapter.id_attribute}"}

      pipeline.push({$group: group_args})
      pipeline.push(sort) if sort # Results must be re-sorted after grouping

      if @_cursor.$one or @hasCursorQuery('$exists')
        pipeline.push({$limit: 1})
      else if @_cursor.$limit
        pipeline.push({$limit: @_cursor.$limit})

      pipeline.push({$skip: @_cursor.$offset}) if @_cursor.$offset

      pipeline.push({$group: {_id: null, count: {$sum: 1}}}) if @_cursor.$count
      cursor_options = {}
      cursor_options.batchSize = fetch if (fetch = @_cursor.$each?.fetch)
      callback(null, {aggregate_cursor: collection.aggregate(pipeline, {cursor: cursor_options})})

  _aggregateMongoCursorToJSON: (aggregate_cursor, callback) ->
    aggregate_cursor.get (err, results) =>
      return callback(err) if err
      if @_cursor.$count
        return callback(null, results[0].count)
      # Clean up id mapping
      for result in results
        result.id = result.__id.toString()
        delete result._id
        delete result.__id
      callback(null, @selectResults(results))
