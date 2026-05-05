# Copyright (c) 2026 Steven Lopez
# SPDX-License-Identifier: LicenseRef-SSAL-1.0
#
# Licensed under the StreamKernel Source Available License (SSAL) v1.0.
# See the LICENSE file in the project root for the full license text.

FROM amazoncorretto:21-alpine
RUN apk add --no-cache wget
WORKDIR /app
COPY build/libs/StreamKernel-0.0.1-SNAPSHOT-all.jar app.jar
CMD ["java", "-Xms2g", "-Xmx2g", "-jar", "app.jar"]
