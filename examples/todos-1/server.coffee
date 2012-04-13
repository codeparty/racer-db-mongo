express = require 'express'
gzip = require 'connect-gzip'
fs = require 'fs'
racer = require 'racer'
shared = require './shared'

racer.use require 'racer-db-mongo'

app = express.createServer()
  .use(express.favicon())
  .use(gzip.staticGzip(__dirname))

store = racer.createStore
  listen: app    # A port or http server
  db:
    type: 'Mongo'
    uri: 'mongo://localhost/derby-todos'

# Clear all existing data on restart
# store.flush() TODO Make this an ARGV switch

# racer.js returns a browserify bundle of the racer client side code and the
# socket.io client side code as well as any additional browserify options
racer.js entry: __dirname + '/client.js', (err, js) ->
  throw err if err
  fs.writeFileSync __dirname + '/script.js', js

app.get '/', (req, res) ->
  res.redirect '/racer'

app.get '/:groupName', (req, res) ->
  model = store.createModel()

  {groupName} = req.params
  groupTodosQuery = model.query('todos').where('groupId').equals(groupName)

  model.subscribe "groups.#{groupName}", groupTodosQuery, (err, group, todos) ->
    model.ref '_group', group
    todoList = model.refList '_todoList', todos, '_group.todoIds'
    unless group.get()
      group.setNull {}
      todoList.push(
        {groupId: groupName, completed: true,  text: 'This one is done already'}
        {groupId: groupName, completed: false, text: 'Example todo'}
        {groupId: groupName, completed: false, text: 'Another example'}
      )

    # Refs must be explicitly declared per model; they are not stored as data
    # model.bundle waits for any pending model operations to complete and then
    # returns the JSON data for initialization on the client
    model.bundle (bundle) ->
      listHtml = (shared.todoHtml todo for todo in model.get '_todoList').join('')
      res.send """
      <!DOCTYPE html>
      <title>Todos</title>
      <link rel=stylesheet href=style.css>
      <body>
      <div id=overlay></div>
      <!-- calling via timeout keeps the page from redirecting if an error is thrown -->
      <form id=head onsubmit="setTimeout(todos.addTodo, 0);return false">
        <h1>Todos</h1>
        <div id=add><div id=add-input><input id=new-todo></div><input id=add-button type=submit value=Add></div>
      </form>
      <div id=dragbox></div>
      <div id=content><ul id=todos>#{listHtml}</ul></div>
      <script>init=#{bundle}</script>
      <script src=https://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js></script>
      <script src=https://ajax.googleapis.com/ajax/libs/jqueryui/1.8.16/jquery-ui.min.js></script>
      <script src=script.js></script>
      """

app.listen 3012
console.log 'Go to http://localhost:3012/racer'
