from domain.service.job.job import JobActivateRequest
from exception.already_exist_scheduler_id_exception import AlreadyExistSchedulerId
from exception.not_exist_scheduler_id_exception import NotExistSchedulerIdException
from scheduler import sched


def add_schedule(request: JobActivateRequest):
    job_id = request.job_id
    import_str = "from {0} import {1}".format("elt_jobs." + job_id, "add_job")
    exec(import_str, globals())
    try:
        add_job()
    except:
        raise AlreadyExistSchedulerId("someting wrong")


def delete_schedule(job_id: str):
    try:
        sched.remove_job(job_id)
    except:
        raise NotExistSchedulerIdException("someting wrong")
