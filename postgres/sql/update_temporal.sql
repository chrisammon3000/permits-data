ALTER TABLE public.permits_raw
	ALTER issue_date TYPE DATE USING issue_date::DATE,
	ALTER status_date TYPE DATE USING status_date::DATE,
	ALTER license_expiration_date TYPE DATE USING license_expiration_date::DATE;