insert into customer (
    `creationDate`,
    `lastModifiedDate`,
    `customerNumber`,
    `emailAddress`,
    `firstname`,
    `lastname`,
    `title`,
    `gender`,
    `uuid`,
    `newsletter`
) values (
    '2019-07-12 10:52:11',
    NULL,
    564232,
    'someone@somewhere.com',
    'Hans',
    'Hansen',
    'DR',
    'MALE',
    '56b579e1-a482-11e9-aa9f-0242ac110004',
    0
);
insert into no_primary_key (
    `creationDate`,
    `lastModifiedDate`,
    `customerNumber`,
    `emailAddress`
) values (
    '2019-07-12 10:52:11',
    NULL,
    564232,
    'someone@somewhere.com'
);
insert into only_primary_key (
    `customerNumber`,
    `emailAddress`
) values (
    564232,
    'someone@somewhere.com'
);