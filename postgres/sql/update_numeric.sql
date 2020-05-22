ALTER TABLE public.permits_raw
	ALTER valuation TYPE NUMERIC(12, 2) USING valuation::NUMERIC(12, 2);