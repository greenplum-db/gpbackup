## How to run tests

```bash
docker build -t gpbackup:test -f arenadata/Dockerfile .
docker run --rm -it --sysctl 'kernel.sem=500 1024000 200 4096' gpbackup:test bash -c "ssh-keygen -A && /usr/sbin/sshd && bash /home/gpadmin/go/src/github.com/greenplum-db/gpbackup/arenadata/run_gpbackup_tests.bash"
```
