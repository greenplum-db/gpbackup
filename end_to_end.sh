#!/usr/bin/env bash
ginkgo -r -randomizeSuites -slowSpecThreshold=10 -noisySkippings=false -randomizeAllSpecs end_to_end 2>&1
