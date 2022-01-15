from aenum import MultiValueEnum


class Behave(MultiValueEnum):
    REPLACE = "replace", 0
    APPEND = "append", 1

    def get_str(self):
        return self.values[0]

    def get_num(self):
        return self.values[1]
