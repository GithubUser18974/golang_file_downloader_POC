# golang file downloader POC

simple poc script in go to test downloading multiple file from the web.

### whats inside
basically inside `main.go` there is 3 differnt methods tested:
1. **sequential** - downloads files one after another (kinda slow)
2. **goroutines + waitgroup** - downloads all files in same time concurrently 
3. **worker pool** - limit how many download run at once with workers so it dont crash or get rate limited

currently `main()` is set to download sample images into `./files` folder using goroutines.

files will be saved in `./files`. you can change the urls inside main.go if you want test your own links.