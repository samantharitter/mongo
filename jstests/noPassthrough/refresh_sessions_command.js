(function() {
    "use strict";

    var conn;
    var admin;
    var startSession = { startSession : 1 };

    conn = MongoRunner.runMongod({nojournal: ""});
    admin = conn.getDB("admin");

    var result = admin.runCommand(startSession);
    assert.commandWorked(result, "failed to startSession");
    var lsid = result.id;

    // Test that we can run refreshSessions unauthenticated if --auth is off.
    result = admin.runCommand({ refreshSessions: [ lsid ] });
    assert.commandWorked(result, "could not run refreshSessions unauthenticated without --auth");

    // Test that we can run refreshSessions authenticated if --auth is off.
    admin.createUser({user: 'user0', pwd: 'password', roles: jsTest.basicUserRoles});
    admin.auth("user0", "password");

    result = admin.runCommand(startSession);
    assert.commandWorked(result, "could not start session");

    var lsid2 = result.id;

    result = admin.runCommand({ refreshSessions: [ lsid2 ] });
    assert.commandWorked(result, "could not run refreshSessions logged in with --auth off");

    // Turn on auth
    MongoRunner.stopMongod(conn);
    conn = MongoRunner.runMongod({auth: "", nojournal: ""});
    admin = conn.getDB("admin");

    admin.createUser({user: 'admin', pwd: 'admin', roles: jsTest.adminUserRoles});
    admin.auth("admin", "admin");
    admin.createUser({user: 'user1', pwd: 'password', roles: jsTest.basicUserRoles});
    admin.logout();


    // Test that we cannot run refreshSessions unauthenticated if --auth is on.
    result = admin.runCommand({ refreshSessions: [ lsid ] });
    assert.commandFailed(result, "able to run refreshSessions without authenticating");

    // Test that we can run refreshSessions on our own sessions authenticated if --auth is on.
    admin.auth("user1", "password");
    result = admin.runCommand(startSession);
    assert.commandWorked(result, "could not start session");

    var lsid3 = result.id;

    result = admin.runCommand({ refreshSessions: [ lsid3 ] });
    assert.commandWorked(result, "unable to run refreshSessions while logged in on admin");

    // Test that we can refresh "others'" sessions (new ones) when authenticated with --auth.
    result = admin.runCommand({ refreshSessions: [ lsid ] });
    assert.commandWorked(result, "unable to refresh novel lsids");

    // Test that sending a mix of known and new sessions is fine
    result = admin.runCommand({ refreshSessions: [ lsid, lsid2, lsid3 ] });
    assert.commandWorked(result, "unable to refresh mix of known and unknown lsids");

    // Test that sending a set of sessions with duplicates is fine
    result = admin.runCommand({ refreshSessions: [ lsid, lsid, lsid, lsid ] });
    assert.commandWorked(result, "unable to refresh with duplicate lsids in the set");

    // Test that we can run refreshSessions with an empty set of sessions.
    result = admin.runCommand({ refreshSessions: [] });
    assert.commandWorked(result, "unable to refresh empty set of lsids");

    MongoRunner.stopMongod(conn);
})();
