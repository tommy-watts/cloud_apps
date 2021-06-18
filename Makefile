# =====================================
#     LOCAL BUILD
#======================================

build:
	docker build . -t cloud_app --build-arg APP=$(APP)

run:
	docker run -p 8080:8080 cloud_app

deploy-app:
	gcloud app deploy apps/$(APP)/app.yaml
