from aenum import MultiValueEnum
class Non_Standard_Cron(MultiValueEnum):
    HOURYLY = "@hourly","0 * * * *"
    DAILY = "@daily","0 0 * * *"
    WEEKLY = "@weekly" ,"0 0 * * 0"
    MONTLY = "@monthly", "0 0 1 * *"
    YEARLY = "@yearly", "0 0 1 1 *"

    def get_description(self):
        return self.values[0]

    def get_cron_expression(self):
        return self.values[1]
