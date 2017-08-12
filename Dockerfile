FROM garland/aws-cli-docker:latest

MAINTAINER Chiranjit Mukherjee mukherjeechiranjit@gmail.com

WORKDIR /
# ENV HOME /

RUN apk add --update alpine-sdk

# install R
RUN apk update \
  && apk add ca-certificates curl \
  && curl --silent \
    --location https://github.com/sgerrand/alpine-pkg-R/releases/download/v3.2.3-r0/R-3.2.3-r0.apk --output /var/cache/apk/R-3.2.3-r0.apk \
    --location https://github.com/sgerrand/alpine-pkg-R/releases/download/v3.2.3-r0/R-dev-3.2.3-r0.apk --output /var/cache/apk/R-dev-3.2.3-r0.apk \
    --location https://github.com/sgerrand/alpine-pkg-R/releases/download/v3.2.3-r0/R-doc-3.2.3-r0.apk --output /var/cache/apk/R-doc-3.2.3-r0.apk \
  && apk add --allow-untrusted \
    /var/cache/apk/R-3.2.3-r0.apk \
    /var/cache/apk/R-dev-3.2.3-r0.apk \
    /var/cache/apk/R-doc-3.2.3-r0.apk \
  && rm -fr /var/cache/apk/*

RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
RUN Rscript -e "install.packages('jsonlite')"

RUN pip install --upgrade pip
RUN pip install dill

# install numpy pandas
RUN apk add --no-cache --virtual=build_dependencies musl-dev gcc python-dev make cmake g++ gfortran && \
    ln -s /usr/include/locale.h /usr/include/xlocale.h && \
    pip install numpy && \
    pip install pandas==0.18.1 && \
    apk del build_dependencies && \
    apk add --no-cache libstdc++ && \
    rm -rf /var/cache/apk/*


# add useful files
ADD driver.sh /
ADD s3utils.R /
ADD s3utils.py /
ADD AWSBatchUtils.R /
ADD AWSBatchUtils.py /
ADD BosonJobMaster.R /
ADD BosonJobMaster.py /

