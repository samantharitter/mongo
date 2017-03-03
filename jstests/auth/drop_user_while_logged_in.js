// Test that privileges are dropped when a user is dropped and re-added while another
// client is logged in under that username.

var conn = MongoRunner.runMongod({ auth: "", smallfiles: "" });
var admin = conn.getDB("admin");

// Add two users
admin.createUser({user: "admin", pwd: "admin", roles: ["read"]})
admin.createUser({user: "userAdmin", pwd: "superSecret", roles: [{role: "userAdminAnyDatabase", db: "admin"}]})

// Test privileges for the first user
assert(admin.auth("admin", "admin"));

// Should be able to read
admin.coll.find();

// Should not be able to write
assert.writeError(admin.coll.insert({ a: 1 }));

// On a new connection, log in as the second user
var conn2 = new Mongo();
var admin2 = conn2.getDB("admin");
admin2.auth("userAdmin", "superSecret")

// Replace the first user with a new user with different privileges
admin2.dropUser("admin")
admin2.createUser({user: "admin", pwd: "topSecret", roles: [{role: "root", db: "admin"}]})

// The first user should not be able to read or write
assert.writeError(admin.coll.insert({ a: 1 }));
assert.throws(function() {
    admin.coll.find();
});
