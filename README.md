ssp - SQLite Streaming Pipe
===========================

Example of use:
```bash
ps ax | ./ssp.py -d' '+ "SELECT * FROM ssp_data"
```

```bash
kill `ps ax | ./ssp.py -d' '+ "SELECT pid FROM ssp_data WHERE COMMAND LIKE 'x-www-browser%'"`
```
