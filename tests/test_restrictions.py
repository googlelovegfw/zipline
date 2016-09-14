import pandas as pd
from pandas.util.testing import assert_series_equal

from zipline.finance.restrictions import (
    RESTRICTION_STATES,
    Restriction,
    InMemoryRestrictions,
    StaticRestrictions,
    NoopRestrictions,
)
from zipline.testing.fixtures import (
    WithDataPortal,
    ZiplineTestCase,
)

str_to_ts = lambda dt_str: pd.Timestamp(dt_str, tz='UTC')


class RestrictionsTestCase(WithDataPortal, ZiplineTestCase):

    ASSET_FINDER_EQUITY_SIDS = 1, 2, 3

    @classmethod
    def init_class_fixtures(cls):
        super(RestrictionsTestCase, cls).init_class_fixtures()
        cls.ASSET1 = cls.asset_finder.retrieve_asset(1)
        cls.ASSET2 = cls.asset_finder.retrieve_asset(2)
        cls.ASSET3 = cls.asset_finder.retrieve_asset(3)

    def test_in_memory_restrictions(self):

        restrictions = [
            Restriction(
                self.ASSET1,
                str_to_ts('2011-01-06'),
                RESTRICTION_STATES.FROZEN
            ),
            Restriction(
                self.ASSET1,
                str_to_ts('2011-01-04'),
                RESTRICTION_STATES.FROZEN
            ),
            Restriction(
                self.ASSET1,
                str_to_ts('2011-01-05'),
                RESTRICTION_STATES.ALLOWED
            ),
            Restriction(
                self.ASSET2,
                str_to_ts('2011-01-03'),
                RESTRICTION_STATES.FROZEN
            ),
        ]

        rlm = InMemoryRestrictions(restrictions)

        # Check the restrictions are ordered by dt for each sid
        self.assertEqual(
            [r.effective_date for r in
             rlm._restrictions_by_asset[self.ASSET1]],
            [str_to_ts(dt_str) for dt_str in
             ('2011-01-04', '2011-01-05', '2011-01-06')]
        )

        self.assertFalse(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-03')))
        self.assertFalse(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-03 14:31')))
        self.assertTrue(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-04')))
        self.assertTrue(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-04 14:31')))
        self.assertFalse(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-05')))
        self.assertFalse(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-05 14:31')))
        self.assertTrue(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-06')))
        self.assertTrue(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-06 14:31')))

        self.assertFalse(rlm.is_restricted(
            self.ASSET3, str_to_ts('2011-01-03')))
        self.assertFalse(rlm.is_restricted(
            self.ASSET3, str_to_ts('2011-01-03 14:31')))
        self.assertFalse(rlm.is_restricted(
            self.ASSET3, str_to_ts('2011-01-04')))
        self.assertFalse(rlm.is_restricted(
            self.ASSET3, str_to_ts('2011-01-04 14:31')))

        assert_series_equal(
            rlm.is_restricted([self.ASSET1, self.ASSET2],
                              str_to_ts('2011-01-03')),
            pd.Series(data={self.ASSET1: False, self.ASSET2: True})
        )
        assert_series_equal(
            rlm.is_restricted([self.ASSET1, self.ASSET2],
                              str_to_ts('2011-01-04')),
            pd.Series(data={self.ASSET1: True, self.ASSET2: True})
        )

    def test_static_restrictions(self):

        rlm = StaticRestrictions([self.ASSET1, self.ASSET2])

        self.assertTrue(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-03')))
        self.assertTrue(
            rlm.is_restricted(self.ASSET2, str_to_ts('2011-01-03')))
        self.assertFalse(
            rlm.is_restricted(self.ASSET3, str_to_ts('2011-01-03')))

        self.assertTrue(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-04')))
        self.assertTrue(
            rlm.is_restricted(self.ASSET2, str_to_ts('2011-01-04')))
        self.assertFalse(
            rlm.is_restricted(self.ASSET3, str_to_ts('2011-01-04')))

        assert_series_equal(
            rlm.is_restricted([self.ASSET1, self.ASSET2],
                              str_to_ts('2011-01-03')),
            pd.Series(data={self.ASSET1: True, self.ASSET2: True})
        )
        assert_series_equal(
            rlm.is_restricted([self.ASSET1, self.ASSET2],
                              str_to_ts('2011-01-04')),
            pd.Series(data={self.ASSET1: True, self.ASSET2: True})
        )

    def test_noop_restrictions(self):
        rlm = NoopRestrictions()

        self.assertFalse(
            rlm.is_restricted(self.ASSET1, str_to_ts('2011-01-03')))
        self.assertFalse(
            rlm.is_restricted(self.ASSET2, str_to_ts('2011-01-03')))

        assert_series_equal(
            rlm.is_restricted([self.ASSET1, self.ASSET2],
                              str_to_ts('2011-01-03')),
            pd.Series(data={self.ASSET1: False, self.ASSET2: False})
        )
