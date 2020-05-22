ALTER TABLE public.permits_raw
	ALTER valuation TYPE NUMERIC(12, 2) USING valuation::numeric(12, 2);