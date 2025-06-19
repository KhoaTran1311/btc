# BTC CRAWLER v1.

To add cronjob:
```bash
crontab -e
```

in the crontab file, edit it as follow:
```
_ _ * * * /path/to/crawl.sh
```
where first _ is minute and second is hour. The script will run everyday
