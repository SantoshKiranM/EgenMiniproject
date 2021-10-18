from crontab import CronTab

cron = CronTab(tab="""
0 9 * * * python Extract_Daily_Main.py
""")

cron.write()
