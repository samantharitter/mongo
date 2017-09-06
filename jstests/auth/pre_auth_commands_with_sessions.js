(function() {
    'use strict';

    var conn = MongoRunner.runMongod({ auth: "", nojournal: "" });
    var admin = conn.getDB("admin");

    admin.createUser({ user: "admin", pwd: "pwd", roles: jsTest.adminUserRoles });
    admin.auth("admin", "pwd");
    admin.createUser({ user: "lily", pwd: "pwd", roles: jsTest.basicUserRoles });
    admin.logout();

    var testCommand = function(cmd) {
        // Test that we can run a pre-auth command without authenticating.
        var command = {};
        command[cmd] = 1;

        assert.commandWorked(admin.runCommand(command));

        // Test that we can authenticate and start a session
        admin.auth("lily", "pwd");
        var res = admin.runCommand({ startSession: 1 });
        assert.commandWorked(res);
        var id = res.id;

        var commandWithSession = {};
        commandWithSession[cmd] = 1;
        commandWithSession["lsid"] = res.id;

        // Test that we can run a pre-auth command with a session while
        // the session owner is logged in (and the session gets ignored)
        assert.commandWorked(admin.runCommand(command), "failed to run command " + cmd + " while logged in");
        assert.commandWorked(admin.runCommand(commandWithSession, "failed to run command " + cmd + " with session while logged in"));

        // Test that we can run a pre-auth command with a session while
        // nobody is logged in (and the session gets ignored)
        admin.logout();
        assert.commandWorked(admin.runCommand(command), "failed to run command " + cmd + " without being logged in");
        assert.commandWorked(admin.runCommand(commandWithSession), "failed to run command " + cmd + " with session without being logged in");

        // Test that we can run a pre-auth command with a session while
        // multiple users are logged in (and the session gets ignored)
        admin.auth("lily", "pwd");
        admin.auth("admin", "pwd");
        assert.commandWorked(admin.runCommand(command), "failed to run command " + cmd + " with multiple users logged in");
        assert.commandWorked(admin.runCommand(commandWithSession), "failed to run command " + cmd + " with session with multiple users logged in");
    };

    var commands = [ "ping", "ismaster" ];
    for (var i = 0; i < commands.length; i++) {
        testCommand(commands[i]);
    }

})();
