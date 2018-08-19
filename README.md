# Cowloon

Multi-tenant database management system.

Cowloon is a management system which realizes ACID and scale-out without pain of distributed system.

# Design
<img src="https://github.com/mrasu/Cowloon/raw/master/docs/images/design.jpeg" width="100">

1. App doesn't need to know which tenant belongs to which database.
2. App must specify a tenant at every query to achieve ACID.
3. One database holds multiple tenants' data.
4. One tenant's data belong to only one database (and replicas).
5. Tenant's data can move between databases when needed.
6. Cowloon decides a database to query. 

# TODO

* Migrate without downtime.
    * Move multiple tables at the same time
    * Change config
    * no downtime
* Run multiple Cowloon servers

and more.

# Inspired by

* [Vitess](https://github.com/vitessio/vitess)
* [gh-ost](https://github.com/github/gh-ost)
