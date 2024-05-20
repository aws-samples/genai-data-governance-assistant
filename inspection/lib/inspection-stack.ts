// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import ecs = require('aws-cdk-lib/aws-ecs');
import s3 = require('aws-cdk-lib/aws-s3');
import ec2 = require('aws-cdk-lib/aws-ec2');
import iam = require('aws-cdk-lib/aws-iam');
import ssm = require('aws-cdk-lib/aws-ssm');
import { aws_opensearchserverless as opensearchserverless } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as customResources from 'aws-cdk-lib/custom-resources';
import { aws_glue as glue } from 'aws-cdk-lib';
import * as lakeformation from 'aws-cdk-lib/aws-lakeformation';

export class InspectionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Bucket for logs and documents
    const bucket = new s3.Bucket(this, 'Bucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      serverAccessLogsPrefix: 'accesslogs',
      publicReadAccess: false,
      enforceSSL: true,
    });

    // VPC
    const vpc = new ec2.Vpc(this, 'VPC', {
      gatewayEndpoints: {
        S3: {
          service: ec2.GatewayVpcEndpointAwsService.S3,
        },
      },
    });
    vpc.addFlowLog('FlowLogS3', {
      destination: ec2.FlowLogDestination.toS3(bucket, 'flowlogs/')
    });
    vpc.addInterfaceEndpoint('EcrDockerEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
    });
    vpc.addInterfaceEndpoint('KmsEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.KMS,
    });

    // Vector database (OpenSearch serverless)
    const sgUseOpensearch = new ec2.SecurityGroup(this, "OpenSearchClientSG", {
      vpc,
      allowAllOutbound: true,
      description: "security group for an opensearch client",
      securityGroupName: "use-opensearch-cluster-sg",
    });
    const sgOpensearchCluster = new ec2.SecurityGroup(this, "OpenSearchSG", {
      vpc,
      allowAllOutbound: true,
      description: "security group for an opensearch cluster",
      securityGroupName: "opensearch-cluster-sg",
    });
    sgOpensearchCluster.addIngressRule(sgOpensearchCluster, ec2.Port.allTcp(), "opensearch-cluster-sg");
    sgOpensearchCluster.addIngressRule(ec2.Peer.ipv4(vpc.vpcCidrBlock), ec2.Port.allTcp(), "vpc-traffic");
    sgOpensearchCluster.addIngressRule(sgUseOpensearch, ec2.Port.tcp(443), "use-opensearch-cluster-sg");
    sgOpensearchCluster.addIngressRule(sgUseOpensearch, ec2.Port.tcpRange(9200, 9300), "use-opensearch-cluster-sg");
    const vectorSecurityPolicy = new opensearchserverless.CfnSecurityPolicy(this, 'VectorSecurityPolicy', {
      name: 'vectorsecuritypolicy',
      policy: '{"Rules":[{"ResourceType":"collection","Resource":["collection/vectordb"]}],"AWSOwnedKey":true}',
      type: 'encryption',
    });
    const privateSubnetIds = vpc.privateSubnets.map(subnet => subnet.subnetId);
    const vectorVpcEndpoint = new opensearchserverless.CfnVpcEndpoint(this, 'VectorVpcEndpoint', {
      name: 'vectordbvpce',
      subnetIds: privateSubnetIds,
      securityGroupIds: [sgOpensearchCluster.securityGroupId],
      vpcId: vpc.vpcId,
    });
    vectorVpcEndpoint.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    const network_security_policy = JSON.stringify([{
      "Rules": [
        {
          "Resource": [
            "collection/vectordb"
          ],
          "ResourceType": "dashboard"
        },
        {
          "Resource": [
            "collection/vectordb"
          ],
          "ResourceType": "collection"
        }
      ],
      "AllowFromPublic": false,
      "SourceVPCEs": [
        vectorVpcEndpoint.attrId
      ]
    }])
    const vectorNetworkPolicy = new opensearchserverless.CfnSecurityPolicy(this, 'VectorNetworkPolicy', {
      name: 'vectornetworkpolicy',
      policy: network_security_policy,
      type: 'network',
    });
    const vectorDB = new opensearchserverless.CfnCollection(this, 'VectorDB', {
      name: 'vectordb',
      description: 'Vector Database',
      standbyReplicas: 'ENABLED',
      type: 'VECTORSEARCH',
    });
    vectorDB.addDependency(vectorSecurityPolicy);
    vectorDB.addDependency(vectorNetworkPolicy);
    const osAdminRole = new iam.Role(this, 'OsAdminRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
    });
    osAdminRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaVPCAccessExecutionRole"));
    osAdminRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'aoss:*',
        ],
        resources: ['*']
      })
    );
    const osInitRole = new iam.Role(this, 'OsInitRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
    });
    osInitRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaVPCAccessExecutionRole"));
    osInitRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'aoss:*',
        ],
        resources: ['*']
      })
    );
    const ecsTaskRoleOS = new iam.Role(this, 'AppRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com')
    });
    ecsTaskRoleOS.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'bedrock:*',
        'aoss:*',
        'glue:*',
        'lakeformation:*'
      ],
      resources: ['*']
    }));
    bucket.grantReadWrite(ecsTaskRoleOS);
    const dataAccessPolicy = JSON.stringify([
      {
        Rules: [
          {
            Resource: [`collection/${vectorDB.name}`],
            Permission: [
              "aoss:*",
            ],
            ResourceType: "collection",
          },
          {
            Resource: [`index/${vectorDB.name}/*`],
            Permission: [
              "aoss:*",
            ],
            ResourceType: "index",
          },
        ],
        Principal: [
          osAdminRole.roleArn,
          osInitRole.roleArn,
          ecsTaskRoleOS.roleArn,
        ],
        Description: "data-access-rule",
      },
    ], null, 2);
    const dataAccessPolicyName = `${vectorDB.name}-policy`;
    const cfnAccessPolicy = new opensearchserverless.CfnAccessPolicy(this, "OpssDataAccessPolicy", {
      name: dataAccessPolicyName,
      description: "Policy for data access",
      policy: dataAccessPolicy,
      type: "data",
    });
    const createOsIndexLambda = new lambda.Function(this, `osIndexCustomResourceLambda`, {
      runtime: lambda.Runtime.PYTHON_3_9,
      vpc: vpc,
      code: lambda.Code.fromAsset("lambda/ossetup", {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
          ],
        },
      }),
      handler: 'lambda_function.on_event',
      tracing: lambda.Tracing.ACTIVE,
      timeout: cdk.Duration.minutes(1),
      memorySize: 1024,
      role: osAdminRole,
      environment: {
        DOMAINURL: vectorDB.attrCollectionEndpoint,
        INDEX: 'embeddings',
        REGION: this.region
      }
    }
    );
    const customResourceProvider = new customResources.Provider(this, `osIndexCustomResourceProvider`, {
      onEventHandler: createOsIndexLambda,
      vpc: vpc,
      role: osInitRole,
    }
    );
    new cdk.CustomResource(this, `customResourceConfigureIndex`, {
      serviceToken: customResourceProvider.serviceToken,
    });

    // ECS cluster
    const cluster = new ecs.Cluster(this, 'Cluster', {
      vpc,
      enableFargateCapacityProviders: true,
      containerInsights: true
    });

    // Data sampling task
    const subsamplerTask = new ecs.FargateTaskDefinition(this, 'SubsamplerTask', {
      memoryLimitMiB: 8192,
      cpu: 2048,
      ephemeralStorageGiB: 200,
    });
    bucket.grantReadWrite(subsamplerTask.taskRole)
    const numRowsParam = new ssm.StringParameter(this, 'NumRowsParameter', {
      parameterName: 'NumRowsParameter',
      stringValue: "100",
      tier: ssm.ParameterTier.ADVANCED,
    });
    const subsamplerContainer = subsamplerTask.addContainer('SubsamplerWorker', {
      image: ecs.ContainerImage.fromAsset('fargate/subsampler'),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'subsampler-log-group', logRetention: 30 }),
      secrets: { 
        NUM_ROWS: ecs.Secret.fromSsmParameter(numRowsParam)
      }
    });

    // Inspection task
    const inspectTask = new ecs.FargateTaskDefinition(this, 'InspectTask', {
      memoryLimitMiB: 8192,
      cpu: 2048,
      taskRole: ecsTaskRoleOS,
      ephemeralStorageGiB: 200,
    });
    const inspectContainer = inspectTask.addContainer('InspectWorker', {
      image: ecs.ContainerImage.fromAsset('fargate/inspector'),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'inspect-log-group', logRetention: 30 }),
    });

    // Duplicate detection task
    const dupdetectTask = new ecs.FargateTaskDefinition(this, 'DuplicateDetectionTask', {
      memoryLimitMiB: 8192,
      cpu: 2048,
      taskRole: ecsTaskRoleOS,
      ephemeralStorageGiB: 200,
    });
    const dupdetectContainer = dupdetectTask.addContainer('DupdetectWorker', {
      image: ecs.ContainerImage.fromAsset('fargate/dupdetector'),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'dupdetect-log-group', logRetention: 30 }),
      environment: {
        'OSS_DOMAIN_ENDPOINT': vectorDB.attrCollectionEndpoint,
        'OSS_INDEX_NAME': 'embeddings',
      }
    });

    // Glue database
    const cfnDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        description: 'Glue database for GenAI curation',
        locationUri: 'locationUri',
        name: 'genaicurateddatabase',
      },
    });
    const lfPermission = new lakeformation.CfnPermissions(this, 'LFPermission', {
      dataLakePrincipal: {
        dataLakePrincipalIdentifier: ecsTaskRoleOS.roleArn
      },
      resource: {
        databaseResource: {
          catalogId: this.account,
          name: cfnDatabase.ref
        },
      },

      permissions: ['ALL'],
      permissionsWithGrantOption: ['ALL'],
    });

    // Curator task
    const curatorTask = new ecs.FargateTaskDefinition(this, 'CuratorTask', {
      memoryLimitMiB: 8192,
      cpu: 2048,
      taskRole: ecsTaskRoleOS,
      ephemeralStorageGiB: 200,
    });
    const curatorContainer = curatorTask.addContainer('CuratorWorker', {
      image: ecs.ContainerImage.fromAsset('fargate/curator'),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'curator-log-group', logRetention: 30 }),
      environment: {
        'OSS_DOMAIN_ENDPOINT': vectorDB.attrCollectionEndpoint,
        'OSS_INDEX_NAME': 'embeddings',
        'GLUE_CATALOG': cfnDatabase.ref
      },
      secrets: { 
        NUM_ROWS: ecs.Secret.fromSsmParameter(numRowsParam)
      }
    });

    // SFN workflow
    const runSubsamplerTaskFirstPass = new tasks.EcsRunTask(this, 'SubsamplerFirstPass', {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      cluster: cluster,
      taskDefinition: subsamplerTask,
      assignPublicIp: false,
      containerOverrides: [{
        containerDefinition: subsamplerContainer,
        environment: [
          { name: 'S3_INPUT', value: sfn.JsonPath.stringAt('$.S3Input') },
          { name: 'S3_OUTPUT', value: sfn.JsonPath.stringAt('$.S3OutputFirst') },
        ],
      }],
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      propagatedTagSource: ecs.PropagatedTagSource.TASK_DEFINITION,
    });
    const runSubsamplerTaskSecondPass = new tasks.EcsRunTask(this, 'SubsamplerSecondPass', {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      cluster: cluster,
      taskDefinition: subsamplerTask,
      assignPublicIp: false,
      containerOverrides: [{
        containerDefinition: subsamplerContainer,
        environment: [
          { name: 'S3_INPUT', value: sfn.JsonPath.stringAt('$.S3Input') },
          { name: 'S3_OUTPUT', value: sfn.JsonPath.stringAt('$.S3OutputSecond') },
        ],
      }],
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      propagatedTagSource: ecs.PropagatedTagSource.TASK_DEFINITION,
    });
    const runInspectTask = new tasks.EcsRunTask(this, 'RunInspectTask', {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      cluster: cluster,
      taskDefinition: inspectTask,
      assignPublicIp: false,
      containerOverrides: [{
        containerDefinition: inspectContainer,
        environment: [
          { name: 'S3_INPUT_1', value: sfn.JsonPath.stringAt('$.S3OutputFirst') },
          { name: 'S3_INPUT_2', value: sfn.JsonPath.stringAt('$.S3OutputSecond') },
          { name: 'S3_OUTPUT_1', value: sfn.JsonPath.stringAt('$.S3OutputSchema1') },
          { name: 'S3_OUTPUT_2', value: sfn.JsonPath.stringAt('$.S3OutputSchema2') },
        ],
      }],
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      propagatedTagSource: ecs.PropagatedTagSource.TASK_DEFINITION,
    });
    const runDupdetectTask = new tasks.EcsRunTask(this, 'RunDupdetectTask', {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      cluster: cluster,
      taskDefinition: dupdetectTask,
      assignPublicIp: false,
      containerOverrides: [{
        containerDefinition: dupdetectContainer,
        environment: [
          { name: 'S3_INPUT', value: sfn.JsonPath.stringAt('$.S3OutputSchema2') },
          { name: 'S3_OUTPUT', value: sfn.JsonPath.stringAt('$.S3OutputDupRank') },
          { name: 'S3_OUTPUT_DESC', value: sfn.JsonPath.stringAt('$.S3OutputTableDesc') },
        ],
      }],
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      propagatedTagSource: ecs.PropagatedTagSource.TASK_DEFINITION,
    });
    const runCuratorTask = new tasks.EcsRunTask(this, 'RunCuratorTask', {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      cluster: cluster,
      taskDefinition: curatorTask,
      assignPublicIp: false,
      containerOverrides: [{
        containerDefinition: curatorContainer,
        environment: [
          { name: 'S3_INPUT_DATA', value: sfn.JsonPath.stringAt('$.S3Input') },
          { name: 'S3_INPUT_SCHEMA', value: sfn.JsonPath.stringAt('$.S3OutputSchema2') },
          { name: 'S3_INPUT_DESC', value: sfn.JsonPath.stringAt('$.S3OutputTableDesc') },
          { name: 'S3_INPUT_DQDL', value: sfn.JsonPath.stringAt('$.S3InputDQDL') },
          { name: 'S3_OUTPUT_DQDL', value: sfn.JsonPath.stringAt('$.S3OutputDQDL') },
          { name: 'TABLE_NAME', value: sfn.JsonPath.stringAt('$.TableName') },
        ],
      }],
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      propagatedTagSource: ecs.PropagatedTagSource.TASK_DEFINITION,
    });
    const wf_chain = sfn.Chain.start(
      runSubsamplerTaskFirstPass).next(
        runSubsamplerTaskSecondPass).next(
          runInspectTask).next(
            runDupdetectTask).next(
              runCuratorTask
            );
    const sfnInspector = new sfn.StateMachine(this, 'StateMachineDataInspector', {
      definition: wf_chain,
      timeout: cdk.Duration.minutes(30),
    });
    sfnInspector.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["ecs:RunTask"],
        resources: [
          subsamplerTask.taskDefinitionArn,
          inspectTask.taskDefinitionArn,
          dupdetectTask.taskDefinitionArn,
          curatorTask.taskDefinitionArn
        ]
      })
    );

    new cdk.CfnOutput(this, 'BucketName', {
      value: `${bucket.bucketName}`,
    });
    new cdk.CfnOutput(this, 'VectorDBDashboard', {
      value: vectorDB.attrDashboardEndpoint
    });
    new cdk.CfnOutput(this, 'GlueDatabaseName', {
      value: cfnDatabase.ref
    });

  }
}
