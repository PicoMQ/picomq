import { runConformanceTests } from '@durable-streams/server-conformance-tests'

runConformanceTests({ baseUrl: process.env.CONFORMANCE_TEST_URL })
