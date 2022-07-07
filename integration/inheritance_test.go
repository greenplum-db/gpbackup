package integration

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo/v2"
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
CREATE TABLE public.parent_inh_test (
    a integer NOT NULL,
    b integer NOT NULL
) DISTRIBUTED BY (a, b);

CREATE TABLE public.child_inh_test (
    a integer NOT NULL,
    b integer NOT NULL
)
INHERITS (public.parent_inh_test) DISTRIBUTED BY (a, b);

INSERT into public.child_inh_test values(1,1);
INSERT into public.child_inh_test values(2,2);
`)
		defer testhelper.AssertQueryRuns(connectionPool, "DROP table public.parent_inh_test")
		defer testhelper.AssertQueryRuns(connectionPool, "DROP table public.child_inh_test")

		sum := dbconn.MustSelectString(connectionPool, fmt.Sprintf("SELECT sum(a) FROM public.parent_inh_test"))
		Expect(sum).To(Equal("3"))
	})
})
