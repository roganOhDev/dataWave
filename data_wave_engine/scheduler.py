from apscheduler.schedulers.background import BackgroundScheduler

sched = BackgroundScheduler(timezone="Asia/Seoul", daemon=True)

sched.start()
print("start")


# def start_scheduler():
