const app = require("express")();
const http = require("http").Server(app);
const io = require("socket.io")(http);
const cassandra = require("cassandra-driver");
const port = 8080;
const refreshRate = 60000;

app.get("/", function (req, res) {
  res.sendFile("index.html", { root: "." });
});

http.listen(port, function () {
  console.log("Running on port " + port);
});

const options = {
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  credentials: { username: 'cassandra', password: 'cassandra' }
};

const cassandraClient = new cassandra.Client(options);
cassandraClient.connect(function (err, result) {
  if (err) {
    console.log("Unable to connect to database: ", err.name);
    return;
  }
  console.log("Connected to database");
});


io.sockets.on("connection", function (socket) {
  console.log("user connected");
  socket.on("disconnect", function () {
    console.log("user disconnected");
  });
});


// Continiously update chart.
setInterval(function () {
  // if (io.engine.clientsCount > 0) {
  cassandraClient.execute(
    "SELECT max(created) AS timestamp, type, CAST(count(1) as text) as count FROM analytics.tweet_types where created >= refreshsub(dateof(now())) group by type ALLOW FILTERING;",
    (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      io.emit("types_data", result.rows);
    }
  );
  // }
}, refreshRate);

// Continiously update chart.
setInterval(function () {
  // if (io.engine.clientsCount > 0) {
  cassandraClient.execute(
    "SELECT max(created) AS timestamp, CAST(count(1) as text) as count FROM analytics.twitter_data where created >= refreshsub(dateof(now())) ALLOW FILTERING;",
    (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      io.emit("tweet_count_data", result.rows[0]);
    }
  );
  // }
}, refreshRate);


// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT max(created) AS timestamp, platform, CAST(count(1) as text) as count FROM analytics.platform_types GROUP BY platform;",
      (err, result) => {
        if (err) {
          console.log(err);
          return;
        }
        io.emit("platform_data", result.rows);
      }
    );
  }
}, refreshRate);

// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT max(created) AS timestamp, language, CAST(count(1) as text) as count FROM analytics.languages GROUP BY language;",
      (err, result) => {
        if (err) {
          console.log(err);
          return;
        }
        io.emit("language_data", result.rows);
      }
    );
  }
}, refreshRate);