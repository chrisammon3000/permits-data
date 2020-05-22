ALTER TABLE public.permits_raw
	ALTER existing_code TYPE SMALLINT USING existing_code::SMALLINT,
	ALTER council_district TYPE SMALLINT USING council_district::SMALLINT,
	ALTER no_of_stories TYPE SMALLINT USING no_of_stories::SMALLINT,
	ALTER assessor_book TYPE SMALLINT USING assessor_book::SMALLINT,
	ALTER address_start TYPE INTEGER USING address_start::INTEGER,
	ALTER no_of_accessory_dwelling_units TYPE SMALLINT USING no_of_accessory_dwelling_units::SMALLINT,
	ALTER assessor_page TYPE SMALLINT USING assessor_page::SMALLINT,
	ALTER proposed_code TYPE SMALLINT USING proposed_code::SMALLINT,
	ALTER address_end TYPE INTEGER USING address_end::INTEGER,
	ALTER project_number TYPE SMALLINT USING project_number::SMALLINT,
	ALTER no_of_residential_dwelling_units TYPE SMALLINT USING no_of_residential_dwelling_units::SMALLINT,
	ALTER license_no TYPE INTEGER USING license_no::INTEGER,
	ALTER zip_code TYPE INTEGER USING zip_code::INTEGER;