ssp - SQLite Streaming Pipe
===========================

Example of use:

Show all processes in table form:

```bash
ps ax | ./ssp.py -d' '+ "SELECT * FROM ssp_data"
```

Kill all processes beginning with x-www-browser:

```bash
kill `ps ax | ./ssp.py -d' '+ "SELECT pid FROM ssp_data WHERE COMMAND LIKE 'x-www-browser%'"`
```
