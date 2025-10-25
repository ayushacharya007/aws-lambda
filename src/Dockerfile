# build image for lamda layer 
FROM public.ecr.aws/lambda/python:3.13
# Copy function code
COPY requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
# Install the function's dependencies using file requirements.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt
# Copy the function code
COPY lambdas/glue_lambda.py ${LAMBDA_TASK_ROOT}/glue_lambda.py
# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "glue_lambda.handler" ]

