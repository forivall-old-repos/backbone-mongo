###
  backbone-mongo.js 0.6.3
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-mongo
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
###

{ObjectID} =  require 'mongodb'
{_, Backbone, Queue, Schema, Utils, JSONUtils} = BackboneORM = require 'backbone-orm'

MongoCursor = require './cursor'
Connection = require './lib/connection'
DatabaseTools = require './database_tools'
mongoConfigureModelType = null

DESTROY_BATCH_LIMIT = 1000
CAPABILITIES = {embed: true, json: true, unique: true, manual_ids: true, dynamic: true, self_reference: true}

class MongoSync

  constructor: (@model_type, @sync_options={}) ->
    @model_type.model_name = Utils.findOrGenerateModelName(@model_type)
    @schema = new Schema(@model_type, {id: {type: ObjectID}})

    @backbone_adapter = @model_type.backbone_adapter = @_selectAdapter()

  # @nodoc
  initialize: (model) ->
    return if @is_initialized; @is_initialized = true

    @schema.initialize()
    throw new Error "Missing url for model" unless url = _.result(new @model_type, 'url')
    @connect(url)

  ###################################
  # Classic Backbone Sync
  ###################################

  # @nodoc
  read: (model, options) ->
    # a collection
    if model.models
      @cursor().toJSON (err, json) ->
        return options.error(err) if err
        options.success(json)

    # a model
    else
      @cursor(model.id).toJSON (err, json) ->
        return options.error(err) if err
        return options.error(new Error "Model not found. Id #{model.id}") unless json
        options.success(json)

  # @nodoc
  create: (model, options, force) ->
    return options.error(new Error("Create should not be called for manual option. Set an id before calling save. Model: #{JSONUtils.stringify(model.toJSON())}")) if @manual_id and not force

    @connection.collection (err, collection) =>
      return options.error(err) if err
      return options.error(new Error 'New document has a non-empty revision') if model.get('_rev')
      doc = @backbone_adapter.attributesToNative(model.toJSON()); doc._rev = 1 # start revisions
      collection.insert doc, (err, docs) =>
        return options.error(new Error "Failed to create model. Error: #{err or 'document not found'}") if err or not docs or docs.length isnt 1
        options.success(@backbone_adapter.nativeToAttributes(docs[0]))

  # @nodoc
  update: (model, options) ->
    return @create(model, options, true) unless model.get('_rev') # no revision, create - in the case we manually set an id and are saving for the first time

    @connection.collection (err, collection) =>
      return options.error(err) if err

      json = @backbone_adapter.attributesToNative(model.toJSON())
      delete json._id if @backbone_adapter.id_attribute is '_id'
      find_query = @backbone_adapter.modelFindQuery(model)
      find_query._rev = json._rev
      json._rev++ # increment revisions

      modifications = {$set: json}
      if unsets = Utils.get(model, 'unsets')
        Utils.unset(model, 'unsets') # clear now that we are dealing with them
        if unsets.length
          modifications.$unset = {}
          modifications.$unset[key] = '' for key in unsets when not model.attributes.hasOwnProperty(key) # unset if they haven't been re-set

      # update the record
      collection.findAndModify find_query, [[@backbone_adapter.id_attribute, 'asc']], modifications, {new: true}, (err, doc) =>
        return options.error(new Error "Failed to update model (#{@model_type.model_name}). Error: #{err}") if err
        return options.error(new Error "Failed to update model (#{@model_type.model_name}). Either the document has been deleted or the revision (_rev) was stale.") unless doc
        return options.error(new Error "Failed to update revision (#{@model_type.model_name}). Is: #{doc._rev} expecting: #{json._rev}") if doc._rev isnt json._rev
        options.success(@backbone_adapter.nativeToAttributes(doc))

  # @nodoc
  delete: (model, options) -> @deleteCB(model, (err) => if err then options.error(err) else options.success())

  # @nodoc
  deleteCB: (model, callback) =>
    @connection.collection (err, collection) =>
      return options.error(err) if err
      collection.remove @backbone_adapter.attributesToNative({id: model.id}), (err) =>
        return callback(err) if err
        Utils.patchRemove(@model_type, model, callback)

  ###################################
  # Backbone ORM - Class Extensions
  ###################################

  # @no_doc
  resetSchema: (options, callback) -> @db().resetSchema(options, callback)

  # @no_doc
  cursor: (query={}) -> return new MongoCursor(query, _.pick(@, ['model_type', 'connection', 'backbone_adapter']))

  # @no_doc
  destroy: (query, callback) ->
    [query, callback] = [{}, query] if arguments.length is 1

    @connection.collection (err, collection) =>
      return callback(err) if err
      @model_type.each _.extend({$each: {limit: DESTROY_BATCH_LIMIT, json: true}}, query), @deleteCB, callback

  ###################################
  # Backbone Mongo - Extensions
  ###################################
  connect: (url) ->
    return if @connection and @connection.url is url
    @connection.destroy() if @connection
    @connection = new Connection(url, @schema, @sync_options)

  collection: (callback) -> @connection.collection(callback)
  db: => @db_tools or= new DatabaseTools(@)

  ###################################
  # Internal
  ###################################
  _selectAdapter: ->
    for field_name, field_info of @schema.raw
      continue if (field_name isnt 'id') or not _.isArray(field_info)
      for info in field_info
        if info.manual or info.manual_id # TODO: remove legacy support for manual_id
          @manual_id = true
          return require './lib/document_adapter_no_mongo_id'
    return require './lib/document_adapter_mongo_id' # default is using the mongodb's ids


module.exports = (type, sync_options={}) ->
  if Utils.isCollection(new type()) # collection
    model_type = Utils.configureCollectionModelType(type, module.exports)
    return type::sync = model_type::sync

  sync = new MongoSync(type, sync_options)
  type::sync = sync_fn = (method, model, options={}) -> # save for access by model extensions
    sync.initialize()
    return module.exports.apply(null, Array::slice.call(arguments, 1)) if method is 'createSync' # create a new sync
    return sync if method is 'sync'
    return sync.schema if method is 'schema'
    return false if method is 'isRemote'
    return if sync[method] then sync[method].apply(sync, Array::slice.call(arguments, 1)) else undefined

  Utils.configureModelType(type) # mixin extensions
  mongoConfigureModelType or= require './extensions/model'
  mongoConfigureModelType(type)
  return BackboneORM.model_cache.configureSync(type, sync_fn)

module.exports.capabilities = (url) -> CAPABILITIES
