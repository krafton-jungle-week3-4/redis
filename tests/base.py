import unittest

import main
from main import expiry_store, redis_store, zset_order_store


class StoreIsolationTestCase(unittest.TestCase):
    """각 테스트가 독립된 메모리 상태에서 시작되게 한다."""

    def setUp(self) -> None:
        redis_store.clear()
        expiry_store.clear()
        zset_order_store.clear()
        main.zset_update_sequence = 0
