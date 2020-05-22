ALTER TABLE public.permits_raw
	ALTER permit_type TYPE VARCHAR(50),
	ALTER license_type TYPE VARCHAR(50),
	ALTER zone TYPE VARCHAR(50),
	ALTER applicant_relationship TYPE VARCHAR(50),
	ALTER permit_sub_type TYPE VARCHAR(50),
	ALTER census_tract TYPE VARCHAR(50),
	ALTER initiating_office TYPE VARCHAR(50),
	ALTER status TYPE VARCHAR(50),
	ALTER permit_category TYPE VARCHAR(50);