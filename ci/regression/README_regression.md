# Regression Testing for `gpbackup`

In order to reuse knowledge contained in the [Greenplum "regress" test suite](https://github.com/greenplum-db/gpdb/tree/master/src/test/regress), where GPDB code is exercised for many of the basic features of Postgres, we do the following:

 * backup using `pg_dump` as file `regression_dump.sql`
 * backup using `gpbackup`
 * restore from the `gpbackup` file using `gprestore`
 * backup using `pg_dump` as file `post_regression_dump.sql`

 * compare `regression_dump.sql` with `regression_dump.sql`

This works well to identify major issues. However, there are many inconsequential differences in the two files as a result of a few inconsequential differences between `pg_dump` and `gpbackup`.

As a result, we "freeze" (committing) the difference file `diff.txt`, and only turn red in the pipeline when the current diff varies from previous diffs.

Is the new diff important, or inconsequential?

Below is a list of the kind of differences that are **unimportant** and should be ignored, by again freezing (committing) a new copy of the diff.  Of course, important changes should *not* be committed. The pipeline job could be frozen until changes are the diff is again inconsequential.


### Ordering mismatches

There are a lot of differences that result from different ordering of items in the backups.

For example, all of this is just a sorting/ordering problem:

```
 CREATE OPERATOR public.= (
     PROCEDURE = public.int8alias1eq,
-    LEFTARG = public.int8alias1,
+    LEFTARG = bigint,
     RIGHTARG = public.int8alias1,
-    COMMUTATOR = OPERATOR(public.=),
     MERGES,
     RESTRICT = eqsel,
     JOIN = eqjoinsel
 );


-ALTER OPERATOR public.= (public.int8alias1, public.int8alias1) OWNER TO gpadmin;
+ALTER OPERATOR public.= (bigint, public.int8alias1) OWNER TO gpadmin;

 --
 -- Name: =; Type: OPERATOR; Schema: public; Owner: gpadmin
 --

 CREATE OPERATOR public.= (
-    PROCEDURE = public.int8alias2eq,
-    LEFTARG = public.int8alias2,
-    RIGHTARG = public.int8alias2,
+    PROCEDURE = public.int8alias1eq,
+    LEFTARG = public.int8alias1,
+    RIGHTARG = public.int8alias1,
     COMMUTATOR = OPERATOR(public.=),
     MERGES,
     RESTRICT = eqsel,
@@ -10211,7 +10210,7 @@
 );


-ALTER OPERATOR public.= (public.int8alias2, public.int8alias2) OWNER TO gpadmin;
+ALTER OPERATOR public.= (public.int8alias1, public.int8alias1) OWNER TO gpadmin;
```

### Deprecated GUC `default_with_oids`

`pg_dump` wraps some tables with `default_with_oids = true` then resets to false afterwards. for example:

```
SET default_with_oids = true;

CREATE TABLE public.emp (
    salary integer,
    manager name
)
INHERITS (public.person) DISTRIBUTED BY (name);

ALTER TABLE public.emp OWNER TO gpadmin;

SET default_with_oids = false;
```

it appears there are about a dozen such places

```
$ ag default_with_oids regression_dump.sql
14:SET default_with_oids = false;
32309:SET default_with_oids = true;
32324:SET default_with_oids = false;
38893:SET default_with_oids = true;
38907:SET default_with_oids = false;
39544:SET default_with_oids = true;
39572:SET default_with_oids = false;
40890:SET default_with_oids = true;
40904:SET default_with_oids = false;
42513:SET default_with_oids = true;
42558:SET default_with_oids = false;
42581:SET default_with_oids = true;
42597:SET default_with_oids = false;
42620:SET default_with_oids = true;
42633:SET default_with_oids = false;
43854:SET default_with_oids = true;
43896:SET default_with_oids = false;
```

`default_with_oids` is [deprecated](https://dba.stackexchange.com/questions/101281/what-is-the-relevance-of-set-default-with-oids-true-in-a-postgresql-dump), and can be ignored

### Inheritance redundancies

`gpbackup` chooses to add redundant column definitions for parent columns in the middle of
child tables.

```
 CREATE TABLE index_constraint_naming.st_pk_inherits (
+    a integer,
+    b integer,
     c integer
 )
 INHERITS (index_constraint_naming.st_pk) DISTRIBUTED BY (a, b);
```

To prove these are harmless, we [added a test](https://github.com/greenplum-db/gpbackup/pull/230) to check that the redundant definition of child columns are *not* masking inherited columns.

Another trivial inconsistency we see concerns the inheritance of constraints. Since the parent table will propogate any assigned constraint to its child tables when restoring, they are not written to the child table's CREATE statements. This, in conjunction with the redundancy inconsistency above, leads to the following diff:

```
 CREATE TABLE public.invalid_check_con_child (
-    CONSTRAINT inh_check_constraint CHECK ((f1 > 0))
+    f1 integer
 )
 INHERITS (public.invalid_check_con) DISTRIBUTED BY (f1);
```


