var request = require('request');
var async = require('async');
var url = require('url');
var fs = require('fs');

var auth = require('./authenticate');
exports.authenticate = auth.getTokens;
exports.authenticate_native = auth.getTokensNative;

var OpenStackStorage = exports.OpenStackStorage = function (pAuthFunction, callback) {
  var self = this;
  self.authFn = pAuthFunction;
  self.tokens = {};
  self.authFn(function (err, res, tokens) {
    if(!err) {
      self.tokens = tokens;
      console.log(tokens);
    }
    callback(err, res, tokens);
  });
};

OpenStackStorage.prototype.getFiles = function (containerName, callback) {
  var self = this;

  var fullPath = containerName.split(/\/(.+)?/);
  var prefix = fullPath[1] || false;
  var prefixObject = {};

  containerName = fullPath[0];

  //Don't include a prefix at all if it does not exist
  if(prefix !== false){
    prefixObject = { prefix: prefix + "%2F", delimiter: "/"};
  }

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);
  
  request(
    {
      method: 'GET',
      uri: targetURL,
      json: {},
      timeout: 1000, 
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Accept": "application/json"
      },
      qs: prefixObject
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        var files = body;
        return callback(err, files);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.getFilesAtPrefix = function (containerName, prefix, callback) {
  var self = this;

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);

  if(prefix.length>0) { 
    if(prefix.substr(-1)!=="/") { prefix = prefix + "/" }
    //if(prefix.substr(-1)!=="/") { prefix = prefix.substr(0, prefix.length-1) }
    if(prefix[0]==="/") { prefix = prefix.substr(1, prefix.length-1) }
  }
  var prefixObject = { prefix: prefix, delimiter: "/"};
  // if(prefix[0]!=="/") { prefix = "/" + prefix; }
  // if(prefix[prefix.length-1]!=="/") { prefix = prefix + "/"; }
  
  request(
    {
      method: 'GET',
      uri: targetURL,
      json: {},
      timeout: 10000, 
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Accept": "application/json"
      },
      qs: prefixObject
    },
    function (err, res, body) {
      console.log(res);
      console.log(body);
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        var files = body;
        console.log(files);
        return callback(err, files);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.getMetadataAtPrefix = function (containerName, prefix, callback) {
  var self = this;

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);

  if(prefix.length>0) { 
    if(prefix.substr(-1)==="/") { prefix = prefix.substr(0, prefix.length-1) }
    if(prefix[0]==="/") { prefix = prefix.substr(1, prefix.length-1) }
  }
  var prefixObject = { prefix: prefix, delimiter: "/"};
  
  request(
    {
      method: 'GET',
      uri: targetURL,
      json: {},
      timeout: 1000, 
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Accept": "application/json"
      },
      qs: prefixObject
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        return callback(err, res.headers);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.getContainers = function (callback) {
  var self = this;
  return self.getFiles("", callback);
};

OpenStackStorage.prototype.createContainer = function (containerName, callback) {
  var self = this;
  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);

  request(
    {
      method: 'PUT',
      uri: targetURL,
      json: {},
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Accept": "application/json"
      }
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        return callback(err, res.statusCode);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.deleteContainer = function (containerName, callback) {
  var self = this;
  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);

  request(
    {
      method: 'DELETE',
      uri: targetURL,
      json: {},
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Accept": "application/json"
      }
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        return callback(err, res.statusCode);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.deleteFile = function (containerName, remoteNameToDelete, callback) {
  var self = this;
  return self.deleteContainer(containerName + '/' + remoteNameToDelete, callback);
};

OpenStackStorage.prototype.putFile = function (containerName, fileToSend, callback) {
  var self = this;
  // if(!fileToSend || !fileToSend.remoteName || (!fileToSend.localFile && !fileToSend.stream) ) {
  //   return callback(new Error("must specify remoteName and either .localFile or .stream for file uploads"));
  // }

  if(fileToSend.remoteName.length>0) { 
    if(fileToSend.remoteName.substr(-1)==="/") { fileToSend.remoteName = fileToSend.remoteName.substr(0, fileToSend.remoteName.length-1) }
    if(fileToSend.remoteName[0]==="/") { fileToSend.remoteName = fileToSend.remoteName.substr(1, fileToSend.remoteName.length-1) }
  }
  
  //fileToSend.remoteName = fileToSend.remoteName.replace(new RegExp("/", 'g'), "%2F");

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName + '/' + fileToSend.remoteName);

  var headers = {
    "X-Auth-Token": self.tokens.id,
    "Accept": "application/json"
    //"X-Object-Manifest": ""
  };
  var fileStream = null;
  if(fileToSend.stream) {
    fileStream = fileToSend.stream;
  } else if (fileToSend.localFile) {
    headers['Content-Length'] = fs.statSync(fileToSend.localFile).size;
    fileStream = fs.createReadStream(fileToSend.localFile);
  }
  
  var uploadStream;

  fileStream.on("finish", function() {
    uploadStream.emit("finish");
  });

  uploadStream = request(
    {
      method: 'PUT',
      uri: targetURL,
      headers: headers
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        //fileStream.pipe(uploadStream);
        callback(err, uploadStream);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
  // fileStream.on("finish", function () { res.emit("finish")})
  fileStream.pipe(uploadStream);
        
  //callback(null,uploadStream);
  //console.log("Oh hi");
  //uploadStream.pause();
};

OpenStackStorage.prototype.getFileMetadata = function (containerName, fileToReceive, callback) {
  var self = this;

  if(!fileToReceive || fileToReceive.length == 0) {
    return callback(new Error("must specify remoteName and a .localFile to get an object's metadata"));
  }

  if(fileToReceive.substr(-1)==="/") { fileToReceive = fileToReceive.substr(0, fileToReceive.length-1) }
  if(fileToReceive[0]==="/") { fileToReceive = fileToReceive.substr(1, fileToReceive.length-1) }

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName + '/' + fileToReceive);

  console.log("Openstack storage pre request");
  console.log(targetURL);
  var headers = {
    "X-Auth-Token": self.tokens.id
  }; 
  request(
    {
      method: 'HEAD',
      uri: targetURL, 
      headers: headers
    },
    function (err, res, body) {
      console.log("Openstack storage post request");
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        return callback(err, res.headers);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.getFile = function (containerName, fileToReceive, callback) {
  var self = this;
  if(!fileToReceive || !fileToReceive.remoteName || (!fileToReceive.localFile && !fileToReceive.stream)) {
    return callback(new Error("must specify remoteName and either .localFile or .stream for file downloads"));
  }

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName + '/' + fileToReceive.remoteName);

  var headers = {
    "X-Auth-Token": self.tokens.id
  };
  var fileStream = null;
  if(fileToReceive.stream) {
    fileStream = fileToReceive.stream;
  } else if (fileToReceive.localFile) {
    fileStream = fs.createWriteStream(fileToReceive.localFile);
  }
  var downloadStream = request(
    {
      method: 'GET',
      uri: targetURL,
      headers: headers
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        return callback(err, res.statusCode);
      } else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
  downloadStream.pipe(fileStream);
};
