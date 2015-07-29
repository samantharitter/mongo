/*    Copyright 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "mongo/client/authenticate.h"

#include "mongo/bson/json.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/config.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/password_digest.h"

// remove
#include <iostream>

namespace mongo {
namespace auth {

using executor::RemoteCommandRequest;
using executor::RemoteCommandResponse;

namespace {

static const char* const kUserSourceFieldName = "userSource";
static const BSONObj kGetNonceCmd = BSON("getnonce" << 1);

bool isOk(const BSONObj& o) {
    return getStatusFromCommandResult(o).isOK();
}

BSONObj getFallbackAuthParams(BSONObj params) {
    if (params["fallbackParams"].type() != Object) {
        return BSONObj();
    }
    return params["fallbackParams"].Obj();
}

std::string extractDBField(const BSONObj& params) {
    return params.hasField(kUserSourceFieldName) ? params[kUserSourceFieldName].valuestr()
                                                 : params[saslCommandUserDBFieldName].valuestr();
}

//
// MONGODB-CR
//

RemoteCommandRequest mongoCRGetNonceCmd(const BSONObj& params) {
    auto request = RemoteCommandRequest();
    request.cmdObj = kGetNonceCmd;
    request.dbname = extractDBField(params);
    return request;
}

RemoteCommandRequest mongoCRAuthenticateCmd(const BSONObj& params, AuthResponse response) {
    std::string username = params[saslCommandUserFieldName].valuestr();
    std::string password = params[saslCommandPasswordFieldName].valuestr();
    bool digest;
    bsonExtractBooleanFieldWithDefault(params, saslCommandDigestPasswordFieldName, true, &digest);

    std::string digested_password = password;
    if (digest)
        digested_password = createPasswordDigest(username, password);

    auto request = RemoteCommandRequest();
    request.dbname = extractDBField(params);

    BSONElement e = response.getValue().data.getField("nonce");
    verify(e.type() == String);
    std::string nonce = e.valuestr();
    BSONObjBuilder b;
    {
        b << "authenticate" << 1 << "nonce" << nonce << "user" << username;
        md5digest d;
        {
            md5_state_t st;
            md5_init(&st);
            md5_append(&st, (const md5_byte_t*)nonce.c_str(), nonce.size());
            md5_append(&st, (const md5_byte_t*)username.c_str(), username.size());
            md5_append(&st, (const md5_byte_t*)password.c_str(), password.size());
            md5_finish(&st, d);
        }
        b << "key" << digestToString(d);
        request.cmdObj = b.done();
    }

    return request;
}

void authMongoCR(RunCommandHook runCommand, const BSONObj& params, AuthCompletionHandler handler) {
    invariant(runCommand && handler);

    // Step 1: send getnonce command, receive nonce
    runCommand(mongoCRGetNonceCmd(params),
               [runCommand, params, handler](AuthResponse response) {

                   // if not ok, call handler now
                   // TODO: safe to call getValue() here?
                   if (!response.isOK() || !isOk(response.getValue().data)) {
                       return handler(response);
                   }

                   // Step 2: send authenticate command, receive response
                   runCommand(mongoCRAuthenticateCmd(params, response), handler);
               });
}

//
// X-509
//

RemoteCommandRequest x509AuthCmd(const BSONObj& params, StringData clientName) {
    auto request = RemoteCommandRequest();
    request.dbname = extractDBField(params);
    request.cmdObj = BSON("authenticate" << 1 << "mechanism"
                                         << "MONGODB-X509"
                                         << "user" << params[saslCommandUserFieldName].valuestr());
    return request;
}

// Use the MONGODB-X509 protocol to authenticate as "username." The certificate details
// has already been communicated automatically as part of the connect call.
void authX509(RunCommandHook runCommand,
              const BSONObj& params,
              StringData clientName,
              AuthCompletionHandler handler) {
    invariant(runCommand && handler);

    if (clientName.toString() == "") {
        return handler(AuthResponse(ErrorCodes::AuthenticationFailed,
                                    "Please enable SSL on the client-side to use the MONGODB-X509 "
                                    "authentication mechanism."));
    }

    if (params[saslCommandUserFieldName].valuestr() != clientName.toString()) {
        StringBuilder message;
        message << "Username \"";
        message << params[saslCommandUserFieldName].valuestr();
        message << "\" does not match the provided client certificate user \"";
        message << clientName.toString() << "\"";
        return handler(AuthResponse(ErrorCodes::AuthenticationFailed, message.str()));
    }

    // Just 1 step: send authenticate command, receive response
    runCommand(x509AuthCmd(params, clientName), handler);
}

//
// General Auth
//

AuthResponse validateParams(const BSONObj& params) {
    std::string mechanism;
    auto response = bsonExtractStringField(params, saslCommandMechanismFieldName, &mechanism);
    if (!response.isOK())
        return response;

    if (mechanism != "MONGODB-CR" && mechanism != "MONGODB-X509" && mechanism != "PLAIN" &&
        mechanism != "GSSAPI" && mechanism != "SCRAM-SHA-1") {
        return AuthResponse(ErrorCodes::InvalidOptions,
                            "Auth mechanism " + mechanism + " not supported.");
    }

    if (params.hasField(saslCommandUserDBFieldName) && params.hasField(kUserSourceFieldName)) {
        return AuthResponse(ErrorCodes::InvalidOptions,
                            "You cannot specify both 'db' and 'userSource'. Please use only 'db'.");
    }

    std::string username;
    response = bsonExtractStringField(params, saslCommandUserFieldName, &username);
    if (!response.isOK())
        return response;

    bool digest;
    response = bsonExtractBooleanFieldWithDefault(
        params, saslCommandDigestPasswordFieldName, true, &digest);
    if (!response.isOK())
        return response;

    std::string password;
    response = bsonExtractStringField(params, saslCommandPasswordFieldName, &password);

    return response;
}

// NOTE: once we enter auth() it is no longer safe to assert, because we may be in the middle
// of asynchronously authenticating. All validation must happen outside of this method.
void auth(RunCommandHook runCommand,
          const BSONObj& params,
          StringData hostname,
          StringData clientName,
          AuthCompletionHandler handler) {
    auto validate = validateParams(params);
    if (!validate.isOK())
        return handler(validate);

    std::string mechanism = params[saslCommandMechanismFieldName].valuestr();

    if (mechanism == StringData("MONGODB-CR", StringData::LiteralTag()))
        return authMongoCR(runCommand, params, handler);

#ifdef MONGO_CONFIG_SSL
    else if (mechanism == StringData("MONGODB-X509", StringData::LiteralTag()))
        return authX509(runCommand, params, clientName, handler);
#endif

    else if (saslClientAuthenticate != nullptr)
        return saslClientAuthenticate(runCommand, hostname, params, handler);

    return handler(AuthResponse(
        ErrorCodes::BadValue, mechanism + " mechanism support not compiled into client library."));
};

bool needsFallback(AuthResponse response) {
    // If we didn't fail, no need to retry
    if (response.isOK())
        return false;

    // If we failed, we fall back for BadValue or CommandNotFound
    auto code = response.getStatus().code();
    return (code == ErrorCodes::BadValue || code == ErrorCodes::CommandNotFound);
}

void asyncAuth(RunCommandHook runCommand,
               const BSONObj& params,
               StringData hostname,
               StringData clientName,
               AuthCompletionHandler handler) {
    auth(runCommand,
         params,
         hostname,
         clientName,
         [runCommand, params, hostname, clientName, handler](AuthResponse response) {
             // If auth failed, try again with fallback params when appropriate
             if (needsFallback(response)) {
                 return auth(
                     runCommand, getFallbackAuthParams(params), hostname, clientName, handler);
             }

             // otherwise, call handler
             return handler(response);
         });
}

}  // namespace

void authenticateClient(const BSONObj& params,
                        StringData hostname,
                        StringData clientName,
                        RunCommandHook runCommand,
                        AuthCompletionHandler handler) {
    if (handler) {
        // Run asynchronously
        return asyncAuth(runCommand, params, hostname, clientName, handler);
    } else {
        // Run synchronously through async framework
        // NOTE: this assumes that runCommand executes synchronously.
        asyncAuth(runCommand,
                  params,
                  hostname,
                  clientName,
                  [](AuthResponse response) {
                      // DBClient expects us to throw in case of an auth error.
                      if (!response.isOK()) {
                          std::cout << "throwing an error, we got an error :(" << std::endl;
                          throw response;
                      }
                  });
    }
}

BSONObj buildAuthParams(StringData dbname,
                        StringData username,
                        StringData passwordText,
                        bool digestPassword) {
    return BSON(saslCommandMechanismFieldName
                << "SCRAM-SHA-1" << saslCommandUserDBFieldName << dbname << saslCommandUserFieldName
                << username << saslCommandPasswordFieldName << passwordText
                << saslCommandDigestPasswordFieldName << digestPassword);
}

StringData getSaslCommandUserDBFieldName() {
    return saslCommandUserDBFieldName;
}

StringData getSaslCommandUserFieldName() {
    return saslCommandUserFieldName;
}

}  // namespace auth
}  // namespace mongo
