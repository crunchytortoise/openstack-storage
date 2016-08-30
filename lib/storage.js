var request = require('request');
var async = require('async');
var path = require('path');
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

  //I'm 99% sure it will always come with a leading slash because of the split above but.... Lets be safe.
  prefix = prefix.length>0 && prefix[0] === "/" ? prefix.substr(1,prefix.length) : prefix

  if(prefix !== false){
    prefixObject = { prefix: path.normalize(prefix + "/"), delimiter: "/"};
  }
  console.log("The prefix object");
  console.log(prefixObject);

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);
  
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

OpenStackStorage.prototype.createPrefix = function (containerName, prefix, callback) {
  var self = this;

  if(prefix.length>0) {
    if(prefix.substr(-1)!=="/") { prefix = prefix + "/" }
    //if(prefix.substr(-1)==="/") { prefix = prefix.substr(0,-1); }
    //if(prefix.substr(-1)!=="/") { prefix = prefix.substr(0, prefix.length-1) }
    if(prefix[0]==="/") { prefix = prefix.substr(1, prefix.length-1) }
  }
  //var prefixObject = { prefix: prefix, delimiter: "/"};
  // if(prefix[0]!=="/") { prefix = "/" + prefix; }
  // if(prefix[prefix.length-1]!=="/") { prefix = prefix + "/"; }

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName + '/' + prefix);

  request(
      {
        method: 'PUT',
        uri: targetURL,
        timeout: 10000,
        headers: {
          "X-Auth-Token": self.tokens.id,
          "Accept": "application/directory"
        },
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

OpenStackStorage.prototype.getFilesAtPrefix = function (containerName, prefix, callback) {
  var self = this;

  if( containerName == "" ) {
    containerName = prefix[0] == "/" ? prefix.split(path.sep)[1] : prefix.split(path.sep)[0];
    prefix = path.normalize(prefix.replace(containerName, ""));
  }

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
        if(typeof res == "undefined") {
          err = new Error("No response");
          callback(err,401);
        }
        else if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.getFilesRecursive = function (containerName, prefix, callback) {
  var self = this;

  if( containerName == "" ) {
    containerName = prefix[0] == "/" ? prefix.split(path.sep)[1] : prefix.split(path.sep)[0];
    prefix = path.normalize(prefix.replace(containerName, ""));
  }

  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName);

  if(prefix.length>0) { 
    if(prefix.substr(-1)!=="/") { prefix = prefix + "/" }
    //if(prefix.substr(-1)!=="/") { prefix = prefix.substr(0, prefix.length-1) }
    if(prefix[0]==="/") { prefix = prefix.substr(1, prefix.length-1) }
  }

  var prefixObject = { prefix: prefix };
  // if(prefix[0]!=="/") { prefix = "/" + prefix; }
  // if(prefix[prefix.length-1]!=="/") { prefix = prefix + "/"; }
  
  request(
    {
      method: 'GET',
      uri: targetURL,
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Accept": "text/plain"
      },
      qs: prefixObject
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        var files = body;
        return callback(err, files);
      } else {
        if(typeof res == "undefined") {
          err = new Error("No response");
          callback(err,401);
        }
        else if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
};

OpenStackStorage.prototype.getMetadataAtPrefix = function (containerName, prefix, callback) {
  var self = this;

  if( containerName == "" ) {
    containerName = prefix.split(path.sep)[0];
    prefix = path.normalize(prefix.replace(containerName, ""));
  }

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
      timeout: 10000, 
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
      timeout: 10000, 
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

OpenStackStorage.prototype.deleteContainer = function (containerName, massDelete, callback) {

  var self = this;
  var targetURL = url.parse(self.tokens.storageUrl + path.normalize('/' + containerName));

  request(
    {
      method: 'DELETE',
      uri: targetURL,
      timeout: 10000, 
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

//This has to be rewritten because we need to do a mass delete
OpenStackStorage.prototype.deleteFile = function (containerName, remoteNameToDelete, callback) {

  var self = this;
  var fileToDelete = path.normalize(containerName + '/' + remoteNameToDelete);

  var targetURL = url.parse(self.tokens.storageUrl + path.normalize('/' + fileToDelete) + '?bulk-delete');

  //Get files that should be deleted
  self.getFilesRecursive(containerName, remoteNameToDelete, function (err, files) {
    if(err) {
      err = new Error("request unsuccessful, statusCode: 550");
    }
    if (!err && files && files.statusCode && files.statusCode >= 200 && files.statusCode <= 204) {
      return callback(err, res.statusCode);
    }
    if(files.length <= 1) {
      request(
        {
          method: 'DELETE',
          uri: targetURL,
          timeout: 10000, 
          body: files,
          headers: {
            "X-Auth-Token": self.tokens.id,
            "Accept": "text/plain"
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
    }
    else {
      //Todo. Change this to massive delete
      request(
        {
          method: 'DELETE',
          uri: targetURL,
          timeout: 10000, 
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
    }
  });
};

// OpenStackStorage.prototype.deleteFile = function (containerName, remoteNameToDelete, callback) {
//   var self = this;
//   var fileToDelete = path.normalize(containerName + '/' + remoteNameToDelete);

//   return self.deleteContainer(fileToDelete, false, callback);
// };

OpenStackStorage.prototype.massDelete = function (containerName, remoteNameToDelete, callback) {
  var self = this;
  var fileToDelete = path.normalize(containerName + '/' + remoteNameToDelete);

  return self.deleteContainer(fileToDelete, true, callback);
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
  console.log("This is the name going into swift");
  console.log(fileToSend.remoteName);

  var targetURL = url.parse(self.tokens.storageUrl + path.normalize('/' + containerName + '/' + fileToSend.remoteName));

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
  // eventEmitter = fileToSend.eventEmitter;
  var uploadStream = request(
    {
      method: 'PUT',
      uri: targetURL,
      headers: headers,
    },
    function (err, res, body) {
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        //fileStream.pipe(uploadStream);
        callback(err, uploadStream);
      } 
      else if (typeof res == "undefined") {
        err = new Error("request unsuccessful, statusCode: 401");
        return callback(err, 401);
      }
      else {
        if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode);
      }
    }
  );
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

  var targetURL = url.parse(self.tokens.storageUrl + path.normalize('/' + containerName + '/' + fileToReceive));

  console.log("Openstack storage pre request");
  console.log(targetURL);
  var headers = {
    "X-Auth-Token": self.tokens.id
  }; 
  request(
    {
      method: 'HEAD',
      uri: targetURL, 
      timeout: 10000, 
      headers: headers
    },
    function (err, res, body) {
      console.log("Openstack storage post request");
      if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
        return callback(err, res.headers);
      } else {
        if(typeof res == "undefined") {
          err = new Error("No response");
          callback(err,401);
        }
        else if(!err) {
          err = new Error("request unsuccessful, statusCode: " + res.statusCode);
        }
        return callback(err, res.statusCode || 401);
      }
    }
  );
};

OpenStackStorage.prototype.getFile = function (containerName, fileToReceive, callback) {
  var self = this;
  if(!fileToReceive || !fileToReceive.remoteName || (!fileToReceive.localFile && !fileToReceive.stream)) {
    return callback(new Error("must specify remoteName and either .localFile or .stream for file downloads"));
  }

  var targetURL = url.parse(self.tokens.storageUrl + path.normalize('/' + containerName + '/' + fileToReceive.remoteName));
  console.log("Full path");
  console.log(targetURL);
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
      headers: headers,
      timeout: 10000, 
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

// OpenStackStorage.prototype.renameFile = function (containerName, fileFrom, fileTo, callback) {
//   var self = this;
//   var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName + '/' + fileFrom);

//   request(
//     {
//       method: 'POST',
//       uri: targetURL,
//       //json: {},
//       form: payLoad,
//       headers: {
//         "X-Auth-Token": self.tokens.id,
//         "X-Object-Meta-name": fileto,
//         "Accept": "application/json"
//       }
//     },
//     function (err, res, body) {
//       if (!err && res && res.statusCode && res.statusCode >= 200 && res.statusCode <= 204) {
//         return callback(err, res.statusCode);
//       } else {
//         if(!err) {
//           err = new Error("request unsuccessful, statusCode: " + res.statusCode);
//         }
//         return callback(err, res.statusCode);
//       }
//     }
//   );
// };

OpenStackStorage.prototype.copyFile = function (containerName, fileFrom, fileTo, callback) {
  var self = this;
  var targetURL = url.parse(self.tokens.storageUrl + '/' + containerName + '/' + fileFrom);

  request(
    {
      method: 'PUT',
      uri: targetURL,
      //json: {},
      timeout: 10000, 
      qs: {"multipart-manifest": "get"},
      headers: {
        "X-Auth-Token": self.tokens.id,
        "Destination": encodeURIComponent("/" + containerName + "/" + fileTo),
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
