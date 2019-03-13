package integration

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("inheritance", func() {

	It("repeating inherited columns in table declaration of child does not mask columns in parent", func() {
		/*
			gpbackup chooses to redundantly define inherited columns in the child.
			This has no effect, as shown below.
			However, it does cause "diffs" in the output of `pg_dump` when compared with gpbackup.
		*/
		testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE parent (
    a integer NOT NULL,
    b integer NOT NULL
) DISTRIBUTED BY (a, b);

CREATE TABLE child (
    a integer NOT NULL,
    b integer NOT NULL,
    c integer NOT NULL
) 
INHERITS (parent) DISTRIBUTED BY (a, b, c);

INSERT into child values(1,1,1);
INSERT into child values(2,2,2);
`)
		defer testhelper.AssertQueryRuns(connectionPool, "DROP table parent")
		defer testhelper.AssertQueryRuns(connectionPool, "DROP table child")

		sum := dbconn.MustSelectString(connectionPool, fmt.Sprintf("SELECT sum(a) FROM parent"))
		Expect(sum).To(Equal("3"))
	})
})
