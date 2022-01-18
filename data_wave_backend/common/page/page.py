from pydantic import BaseModel


class Page(BaseModel):
    page_number: int
    page_size: int

    def get_pageable(self) -> (int, int):
        return self.page_size * self.page_number + 1, self.page_size * (self.page_number + 1)
