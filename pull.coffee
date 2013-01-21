r = require 'rethinkdb'
http = require 'http'
util = require 'util'
xml2js = require 'xml2js'

class Database
    default_server:
        host: 'localhost'
        port: '28015'

    # Constructor
    # options = {
    #   callback_on_connect (executed once connected to the database)
    #   host
    #   port
    # }
    constructor: (options) ->
        @callback_on_connect = options.callback_on_connect

        @server = {}
        @server.host = if options.host? then options.host else @default_server.host
        @server.port = if options.port? then options.port else @default_server.port

        @connect()

    error_on_connect: =>
        util.log 'Failed to connect to the database'
        process.exit()

    on_connect: =>
        util.log 'Connection established'
        @callback_on_connect()
    
    # Connect to the database
    connect: =>
        r.connect @server, @on_connect, @error_on_connect



class MuniApi
    constructor: ->
        @parser = new xml2js.Parser()
   

    # Fetch data from an url
    # args = {
    #  host
    #  path
    #  callback (executed once we get the response)
    # }
    fetch_url: (args) =>
        options = {
            host: args.host
            path: args.path
        }
        
        # Define the main callback
        callback = (response) ->
            try
                str = ''
                response.on 'data',  (chunk) ->
                    str += chunk

                response.on 'end', ->
                    if args.callback?
                        args.callback(str)
            catch err
                #TODO write a more useful catch
                console.log JSON.stringify err


        try
            http.request(options, callback).end()
        catch err
            #TODO write a more useful catch
            console.log JSON.stringify err


    # Main method that is going to trigger everything
    # args = {
    #  line (to monitor)
    # }
    monitor: (args) =>
        util.log 'Start monitoring...'
        that = @
        @get_stops
            line: args.line
            callback: (args) ->
                callback = ->
                    that.get_prediction
                        line: args.line
                        stops: args.stops

                callback()
                setInterval callback, 20*1000 # We pull data every 20 seconds

    # Get the stops of a line
    # args = {
    #  line
    #  callback
    # }
    get_stops: (args) =>
        util.log 'Getting stops...'
        that = @
        @fetch_url
            host: 'webservices.nextbus.com'
            path: '/service/publicXMLFeed?command=routeConfig&a=sf-muni&r='+args.line
            callback: (result) ->
                that.parser.parseString result, (err, parsed_results) ->
                    raw_stops = parsed_results?['body']?['route']?[0]?['stop']
                    stops = []
                    if not raw_stops?
                        util.log 'Could not retrieve stops.'
                        process.exit()
                    for stop in raw_stops
                        if /^[0-9]+$/.test(stop['$'].tag) is true # For safety, but not really needed...
                            stops.push stop['$'].tag

                    util.log 'Got '+stops.length+' stops.'
                    args.callback
                        line: args.line
                        stops: stops

    # Get the predictions of a group of stops
    # args = {
    #  line
    #  stops
    #  callback
    # }
    get_prediction: (args) =>
        util.log 'Getting prediction....'
        stops_url = ''
        for stop in args.stops
            stops_url += '&stops='+args.line+'|'+stop
        that = @
        @fetch_url
            host: 'webservices.nextbus.com'
            path: '/service/publicXMLFeed?command=predictionsForMultiStops&a=sf-muni'+stops_url
            callback: (result) ->
                util.log 'Got prediction.'
                that.parser.parseString result, (err, parsed_result) ->
                    predictions_to_insert = []
                    now = Date.now()
                    predictions = parsed_result?['body']?['predictions']
                    if predictions?
                        for prediction, i in predictions
                            stop_tag = prediction['$']?['stopTag']
                            next_bus_sec = prediction['direction']?[0]?['prediction']?[0]?['$']?['seconds']
                            next_bus_min = prediction['direction']?[0]?['prediction']?[0]?['$']?['minutes']
                            vehicle = prediction['direction']?[0]?['prediction']?[0]?['$']?['vehicle']
                            if next_bus_sec? and next_bus_min? and stop_tag? and vehicle?
                                predictions_to_insert.push
                                    stop_tag: parseInt stop_tag
                                    next_bus_sec: parseInt next_bus_sec
                                    next_bus_min: parseInt next_bus_min
                                    vehicle: parseInt vehicle
                                    time: now
                        cursor = r.db('muni').table('raw').insert(predictions_to_insert).run()
                        cursor.collect (data) ->
                            util.log 'Inserted: '+data[0].inserted+', errors: '+data[0].errors
                    else # predictions?
                        util.log 'Failed to parse body'

# Main method that we are going to execute
main = ->
    util.log 'Starting server...'
    database = new Database
        callback_on_connect: ->
            muni_api = new MuniApi()
            muni_api.monitor
                line: 48 # Let's monitor line 48 (the one I use the most)

main()
