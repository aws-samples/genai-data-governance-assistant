# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Generate sample assets and subscription requests.
"""

from faker import Faker
import boto3
import random
import time

fake = Faker()
dzclient = boto3.client('datazone')
departments = ['Sales', 'Marketing', 'Engineering', 'Finance']
domainId = 'DOMAIN'
projects = ['PROJECT1', 'PROJECT2', 'PROJECT3']
num_asset_types_per_project = 3
num_assets_per_type = 10
pct_accepted = 0.75

def create_custom_asset_type(projectId):
    department = random.choice(departments)
    uid = fake.first_name()
    form_name=f"{uid}{department}FormType"
    asset_type_name=f"{uid}{department}AssetType"
    smithy_model = """

structure SageMakerModelFormType {
   @required
   @amazon.datazone#searchable
   modelName: String

   @required
   modelArn: String

   @required
   creationTime: String
}
"""
    smithy_model = smithy_model.replace('SageMakerModelFormType', form_name)

    ft_response = dzclient.create_form_type(
        description=f"Form for custom data for department {department} and user {uid}",
        domainIdentifier=domainId,
        model={
            'smithy': smithy_model,
        },
        name=form_name,
        owningProjectIdentifier=projectId,
        status='ENABLED'
    )

    at_response = dzclient.create_asset_type(
        description=f"Asset type for custom data for department {department} and user {uid}",
        domainIdentifier=domainId,
        formsInput={
            'SubscriptionTerms': {
                'required': False,
                'typeIdentifier': 'amazon.datazone.SubscriptionTermsFormType',
                'typeRevision': '1'
            },
            'CustomMetadata': {
                'required': False,
                'typeIdentifier': form_name,
                'typeRevision': ft_response['revision']
            }
        },
        name = asset_type_name,
        owningProjectIdentifier=projectId
    )

    return form_name, asset_type_name, at_response['revision'], ft_response['revision']

def create_custom_asset(idx, asset_type, asset_type_revision, form_name, form_revision, projectId):
    description=f"Custom asset {idx} for asset type {asset_type} and form {form_name}"
    asset_name=f"{asset_type}{idx}"
    a_response = dzclient.create_asset(
        description=description,
        domainIdentifier=domainId,
        formsInput=[
            {
                'content': "{\n \"modelName\" : \"sample-ModelName\",\n \"modelArn\" : \"999999911111\", \n \"creationTime\" : \"2024-01-01\"\n}",
                'formName': form_name,
                'typeIdentifier': form_name,
                'typeRevision': form_revision, 
            },
        ],
        name=asset_name,
        owningProjectIdentifier=projectId,
        typeIdentifier=asset_type,
        typeRevision=asset_type_revision
    )

    return asset_name, a_response['revision'], a_response['id'] 

def create_listing(asset_id, asset_rev):
    response = dzclient.create_listing_change_set(
        action='PUBLISH',
        domainIdentifier=domainId,
        entityIdentifier=asset_id,
        entityRevision=asset_rev,
        entityType='ASSET'
    )

    return response['listingId']

def create_subscription_request(listing_id, other_project_id):
    response = dzclient.create_subscription_request(
        domainIdentifier=domainId,
        requestReason=fake.bs(),
        subscribedListings=[
            {
                'identifier': listing_id,
            },
        ],
        subscribedPrincipals=[
            {
                'project': {
                    'identifier': other_project_id
                }
            },
        ]
    )
    return response['id']

def get_sub_project(projectId):
    # Given one project, find a random selection of another project that isn't the same as the first
    sub_project = projectId
    while sub_project == projectId:
        sub_project = random.choice(projects)

    return sub_project

def approve_subscription_request(request_id):
    dzclient.accept_subscription_request(
        domainIdentifier=domainId,
        decisionComment=fake.bs(),
        identifier=request_id
    )   

def reject_subscription_request(request_id):
    dzclient.reject_subscription_request(
        domainIdentifier=domainId,
        decisionComment=fake.bs(),
        identifier=request_id
    )

def get_listing_status(listingId):
    response = dzclient.get_listing(
        domainIdentifier=domainId,
        identifier=listingId,
    )
    return response['status']

if __name__ == '__main__':
    print("Starting")

    for p in projects:
        for i in range(num_asset_types_per_project):
            form_name, asset_type, asset_type_revision, form_revision = create_custom_asset_type(p)
            print(f"Created form {form_name} and asset type {asset_type}")
            for j in range(num_assets_per_type):
                asset_name, asset_revision, asset_id = create_custom_asset(j, asset_type, asset_type_revision, form_name, form_revision, p)
                print(f"Created asset {asset_name}")

                listing_id = create_listing(asset_id, asset_revision)
                print(f"Created listing {listing_id}")

                listing_status = 'CREATING'
                while listing_status != 'ACTIVE':
                    listing_status = get_listing_status(listing_id)
                    print(f"Listing status is {listing_status}")
                    time.sleep(10)

                sub_project = get_sub_project(p)
                request_id = create_subscription_request(listing_id, sub_project)
                print(f"Created request {request_id} for asset {asset_name} and project {sub_project}")

                if random.random() > pct_accepted:
                    print("Rejecting request")
                    reject_subscription_request(request_id)
                else:
                    print("Approving request"   )
                    approve_subscription_request(request_id)