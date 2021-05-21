# =====================================
#     LOCAL BUILD
#======================================

build:
	docker build . -t dash_app --build-arg APP=$(APP)

run:
	docker run -p 8080:8080 dash_app

deploy-app:
	gcloud app deploy apps/$(APP)/app.yaml
