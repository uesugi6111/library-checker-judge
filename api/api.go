package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"time"

	"github.com/go-playground/validator/v10"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	pb "github.com/yosupo06/library-checker-judge/api/proto"
)

func (s *server) Register(ctx context.Context, in *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if in.Name == "" {
		return nil, errors.New("empty user name")
	}
	if in.Password == "" {
		return nil, errors.New("empty password")
	}
	passHash, err := bcrypt.GenerateFromPassword([]byte(in.Password), 10)
	if err != nil {
		return nil, errors.New("bcrypt broken")
	}
	user := User{
		Name:     in.Name,
		Passhash: string(passHash),
	}
	if err := s.db.Create(&user).Error; err != nil {
		return nil, errors.New("this username are already registered")
	}
	token, err := s.authTokenManager.IssueToken(user)
	if err != nil {
		return nil, errors.New("broken")
	}
	return &pb.RegisterResponse{
		Token: token,
	}, nil
}

func (s *server) Login(ctx context.Context, in *pb.LoginRequest) (*pb.LoginResponse, error) {
	var user User
	if err := s.db.Where("name = ?", in.Name).Take(&user).Error; err != nil {
		return nil, errors.New("invalid username")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.Passhash), []byte(in.Password)); err != nil {
		return nil, errors.New("invalid password")
	}

	token, err := s.authTokenManager.IssueToken(user)
	if err != nil {
		return nil, err
	}
	return &pb.LoginResponse{
		Token: token,
	}, nil
}

func (s *server) UserInfo(ctx context.Context, in *pb.UserInfoRequest) (*pb.UserInfoResponse, error) {
	name := ""
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	myName := currentUserName
	if in.Name != "" {
		name = in.Name
	} else {
		name = myName
	}
	if name == "" {
		return nil, errors.New("empty name")
	}
	user, err := fetchUser(s.db, name)
	if err != nil {
		return nil, errors.New("invalid user name")
	}
	stats, err := fetchUserStatistics(s.db, name)
	if err != nil {
		return nil, errors.New("failed to fetch statistics")
	}
	respUser := &pb.User{
		Name:       name,
		IsAdmin:    user.Admin,
		Email:      user.Email,
		LibraryUrl: user.LibraryURL,
	}

	if in.Name != myName && !currentUser.Admin {
		respUser.Email = ""
	}

	resp := &pb.UserInfoResponse{
		IsAdmin: user.Admin,
		User:    respUser,
	}
	resp.SolvedMap = make(map[string]pb.SolvedStatus)
	for key, value := range stats {
		resp.SolvedMap[key] = value
	}
	return resp, nil
}

func (s *server) UserList(ctx context.Context, in *pb.UserListRequest) (*pb.UserListResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	if currentUser.Name == "" {
		return nil, errors.New("not login")
	}
	if !currentUser.Admin {
		return nil, errors.New("must be admin")
	}
	users := []User{}
	if err := s.db.Select("name, admin").Find(&users).Error; err != nil {
		return nil, errors.New("failed to get users")
	}
	res := &pb.UserListResponse{}
	for _, user := range users {
		res.Users = append(res.Users, &pb.User{
			Name:    user.Name,
			IsAdmin: user.Admin,
		})
	}
	return res, nil
}

func (s *server) ChangeUserInfo(ctx context.Context, in *pb.ChangeUserInfoRequest) (*pb.ChangeUserInfoResponse, error) {
	type NewUserInfo struct {
		Email      string `validate:"omitempty,email,lt=50"`
		LibraryURL string `validate:"omitempty,url,lt=200"`
	}
	name := in.User.Name
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)

	if currentUser.Name == "" {
		return nil, errors.New("not login")
	}
	if name == "" {
		return nil, errors.New("requested name is empty")
	}
	if name != currentUser.Name && !currentUser.Admin {
		return nil, errors.New("permission denied")
	}
	if name == currentUser.Name && currentUser.Admin && !in.User.IsAdmin {
		return nil, errors.New("cannot remove myself from admin group")
	}

	userInfo := &NewUserInfo{
		Email:      in.User.Email,
		LibraryURL: in.User.LibraryUrl,
	}
	if err := validator.New().Struct(userInfo); err != nil {
		return nil, err
	}

	if err := updateUser(s.db, User{
		Name:       in.User.Name,
		Admin:      in.User.IsAdmin,
		Email:      userInfo.Email,
		LibraryURL: userInfo.LibraryURL,
	}); err != nil {
		return nil, err
	}

	return &pb.ChangeUserInfoResponse{}, nil
}

func (s *server) ProblemInfo(ctx context.Context, in *pb.ProblemInfoRequest) (*pb.ProblemInfoResponse, error) {
	name := in.Name
	if name == "" {
		return nil, errors.New("empty problem name")
	}
	var problem Problem
	if err := s.db.Select("name, title, statement, timelimit, testhash, source_url").Where("name = ?", name).Take(&problem).Error; err != nil {
		return nil, errors.New("failed to get problem")
	}

	return &pb.ProblemInfoResponse{
		Title:       problem.Title,
		Statement:   problem.Statement,
		TimeLimit:   float64(problem.Timelimit) / 1000.0,
		CaseVersion: problem.Testhash,
		SourceUrl:   problem.SourceUrl,
	}, nil
}

func (s *server) ChangeProblemInfo(ctx context.Context, in *pb.ChangeProblemInfoRequest) (*pb.ChangeProblemInfoResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	if !currentUser.Admin {
		return nil, errors.New("must be admin")
	}
	name := in.Name
	if name == "" {
		return nil, errors.New("empty problem name")
	}
	var problem Problem
	err := s.db.Select("name, title, statement, timelimit").Where("name = ?", name).First(&problem).Error
	problem.Name = name
	problem.Title = in.Title
	problem.Timelimit = int32(in.TimeLimit * 1000.0)
	problem.Statement = in.Statement
	problem.Testhash = in.CaseVersion
	problem.SourceUrl = in.SourceUrl

	if errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("add problem: %v", name)
		if err := s.db.Create(&problem).Error; err != nil {
			return nil, errors.New("failed to insert")
		}
	} else if err != nil {
		log.Print(err)
		return nil, errors.New("connect to db failed")
	}
	if err := s.db.Model(&Problem{}).Where("name = ?", name).Updates(problem).Error; err != nil {
		return nil, errors.New("failed to update user")
	}
	return &pb.ChangeProblemInfoResponse{}, nil
}

func (s *server) ProblemList(ctx context.Context, in *pb.ProblemListRequest) (*pb.ProblemListResponse, error) {
	problems := []Problem{}
	if err := s.db.Select("name, title").Find(&problems).Error; err != nil {
		return nil, errors.New("fetch problems failed")
	}

	res := pb.ProblemListResponse{}
	for _, prob := range problems {
		res.Problems = append(res.Problems, &pb.Problem{
			Name:  prob.Name,
			Title: prob.Title,
		})
	}
	return &res, nil
}

func (s *server) Submit(ctx context.Context, in *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	if in.Source == "" {
		return nil, errors.New("empty Source")
	}
	if len(in.Source) > 1024*1024 {
		return nil, errors.New("too large Source")
	}
	ok := false
	for _, lang := range s.langs {
		if lang.Id == in.Lang {
			ok = true
			break
		}
	}
	if !ok {
		return nil, errors.New("unknown Lang")
	}
	if _, err := s.ProblemInfo(ctx, &pb.ProblemInfoRequest{
		Name: in.Problem,
	}); err != nil {
		log.Print(err)
		return nil, errors.New("unknown problem")
	}
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	name := currentUser.Name
	submission := Submission{
		ProblemName: in.Problem,
		Lang:        in.Lang,
		Status:      "WJ",
		Source:      in.Source,
		MaxTime:     -1,
		MaxMemory:   -1,
		UserName:    sql.NullString{String: name, Valid: name != ""},
	}

	if err := s.db.Create(&submission).Error; err != nil {
		log.Print(err)
		return nil, errors.New("Submit failed")
	}

	if err := toWaitingJudge(s.db, submission.ID, 50, time.Duration(0)); err != nil {
		log.Print(err)
		return nil, errors.New("inserting to judge queue is failed")
	}

	log.Println("Submit ", submission.ID)

	return &pb.SubmitResponse{Id: submission.ID}, nil
}

func canRejudge(currentUser User, submission *pb.SubmissionOverview) bool {
	name := currentUser.Name
	if name == "" {
		return false
	}
	if name == submission.UserName {
		return true
	}
	if !submission.IsLatest && submission.Status == "AC" {
		return true
	}
	if currentUser.Admin {
		return true
	}
	return false
}

func (s *server) SubmissionInfo(ctx context.Context, in *pb.SubmissionInfoRequest) (*pb.SubmissionInfoResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	var sub Submission
	sub, err := fetchSubmission(s.db, in.Id)
	if err != nil {
		return nil, err
	}
	var cases []SubmissionTestcaseResult
	if err := s.db.Where("submission = ?", in.Id).Find(&cases).Error; err != nil {
		return nil, errors.New("Submission fetch failed")
	}
	overview, err := toProtoSubmission(&sub)
	if err != nil {
		log.Print(err)
		return nil, err
	}

	res := &pb.SubmissionInfoResponse{
		Overview:     overview,
		Source:       sub.Source,
		CompileError: sub.CompileError,
		CanRejudge:   canRejudge(currentUser, overview),
	}

	sort.Slice(cases, func(i, j int) bool {
		return cases[i].Testcase < cases[j].Testcase
	})

	for _, c := range cases {
		res.CaseResults = append(res.CaseResults, &pb.SubmissionCaseResult{
			Case:   c.Testcase,
			Status: c.Status,
			Time:   float64(c.Time) / 1000.0,
			Memory: int64(c.Memory),
		})
	}
	return res, nil
}

func (s *server) SubmissionList(ctx context.Context, in *pb.SubmissionListRequest) (*pb.SubmissionListResponse, error) {
	if 1000 < in.Limit {
		in.Limit = 1000
	}

	filter := &Submission{
		ProblemName: in.Problem,
		Status:      in.Status,
		Lang:        in.Lang,
		UserName:    sql.NullString{String: in.User, Valid: (in.User != "")},
		Hacked:      in.Hacked,
	}

	count := int64(0)
	if err := s.db.Model(&Submission{}).Where(filter).Count(&count).Error; err != nil {
		return nil, errors.New("count query failed")
	}
	order := ""
	if in.Order == "" || in.Order == "-id" {
		order = "id desc"
	} else if in.Order == "+time" {
		order = "max_time asc"
	} else {
		return nil, errors.New("unknown sort order")
	}

	var submissions = make([]Submission, 0)
	if err := s.db.Where(filter).Limit(int(in.Limit)).Offset(int(in.Skip)).
		Preload("User", func(db *gorm.DB) *gorm.DB {
			return db.Select("name")
		}).
		Preload("Problem", func(db *gorm.DB) *gorm.DB {
			return db.Select("name, title, testhash")
		}).
		Select("id, user_name, problem_name, lang, status, hacked, testhash, max_time, max_memory").
		Order(order).
		Find(&submissions).Error; err != nil {
		return nil, errors.New("select query failed")
	}

	res := pb.SubmissionListResponse{
		Count: int32(count),
	}
	for _, sub := range submissions {
		protoSub, err := toProtoSubmission(&sub)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		res.Submissions = append(res.Submissions, protoSub)
	}
	return &res, nil
}

func (s *server) Rejudge(ctx context.Context, in *pb.RejudgeRequest) (*pb.RejudgeResponse, error) {
	sub, err := s.SubmissionInfo(ctx, &pb.SubmissionInfoRequest{Id: in.Id})
	if err != nil {
		return nil, err
	}
	if !sub.CanRejudge {
		return nil, errors.New("no permission")
	}
	if err := toWaitingJudge(s.db, in.Id, 40, time.Duration(0)); err != nil {
		log.Print(err)
		return nil, errors.New("cannot insert into queue")
	}
	return &pb.RejudgeResponse{}, nil
}

func (s *server) LangList(ctx context.Context, in *pb.LangListRequest) (*pb.LangListResponse, error) {
	return &pb.LangListResponse{Langs: s.langs}, nil
}

func (s *server) Ranking(ctx context.Context, in *pb.RankingRequest) (*pb.RankingResponse, error) {
	type Result struct {
		UserName string
		AcCount  int
	}
	var results = make([]Result, 0)
	if err := s.db.
		Model(&Submission{}).
		Select("user_name, count(distinct problem_name) as ac_count").
		Where("status = 'AC' and user_name is not null").
		Group("user_name").
		Find(&results).Error; err != nil {
		log.Print(err)
		return nil, errors.New("failed sql query")
	}
	stats := make([]*pb.UserStatistics, 0)
	for _, result := range results {
		stats = append(stats, &pb.UserStatistics{
			Name:  result.UserName,
			Count: int32(result.AcCount),
		})
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Count != stats[j].Count {
			return stats[i].Count > stats[j].Count
		}
		return stats[i].Name < stats[j].Name
	})
	res := pb.RankingResponse{
		Statistics: stats,
	}
	return &res, nil
}

func (s *server) PopJudgeTask(ctx context.Context, in *pb.PopJudgeTaskRequest) (*pb.PopJudgeTaskResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	if !currentUser.Admin {
		return nil, errors.New("permission denied")
	}
	if in.JudgeName == "" {
		return nil, errors.New("JudgeName is empty")
	}
	for i := 0; i < 10; i++ {
		task, err := popTask(s.db)
		if err != nil {
			return nil, err
		}
		if task.Submission == -1 {
			// Judge queue is empty
			return &pb.PopJudgeTaskResponse{
				SubmissionId: -1,
			}, nil
		}
		id := task.Submission

		expectedTime := in.ExpectedTime.AsDuration()
		if !in.ExpectedTime.IsValid() {
			expectedTime = time.Minute
		}
		log.Println("Pop Submission:", id, expectedTime)

		if err := registerSubmission(s.db, id, in.JudgeName, expectedTime, Waiting); err != nil {
			log.Print(err)
			continue
		}
		if err := pushTask(s.db, Task{
			Submission: id,
			Priority:   task.Priority + 1,
			Available:  time.Now().Add(expectedTime),
		}); err != nil {
			log.Print(err)
			return nil, err
		}

		log.Print("Clear SubmissionTestcaseResults: ", id)
		if err := s.db.Where("submission = ?", id).Delete(&SubmissionTestcaseResult{}).Error; err != nil {
			log.Println(err)
			return nil, errors.New("failed to clear submission testcase results")
		}
		return &pb.PopJudgeTaskResponse{
			SubmissionId: task.Submission,
		}, nil
	}
	log.Println("Too many invalid tasks")
	return &pb.PopJudgeTaskResponse{
		SubmissionId: -1,
	}, nil
}

func (s *server) SyncJudgeTaskStatus(ctx context.Context, in *pb.SyncJudgeTaskStatusRequest) (*pb.SyncJudgeTaskStatusResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	if !currentUser.Admin {
		return nil, errors.New("permission denied")
	}
	if in.JudgeName == "" {
		return nil, errors.New("JudgeName is empty")
	}
	id := in.SubmissionId

	expectedTime := in.ExpectedTime.AsDuration()
	if !in.ExpectedTime.IsValid() {
		expectedTime = time.Minute
	}

	if err := updateSubmissionRegistration(s.db, id, in.JudgeName, expectedTime); err != nil {
		log.Println(err)
		return nil, err
	}

	for _, testCase := range in.CaseResults {
		if err := s.db.Create(&SubmissionTestcaseResult{
			Submission: id,
			Testcase:   testCase.Case,
			Status:     testCase.Status,
			Time:       int32(testCase.Time * 1000),
			Memory:     testCase.Memory,
		}).Error; err != nil {
			log.Println(err)
			return nil, errors.New("DB update failed")
		}
	}
	if err := s.db.Model(&Submission{
		ID: id,
	}).Updates(&Submission{
		Status:       in.Status,
		MaxTime:      int32(in.Time * 1000),
		MaxMemory:    in.Memory,
		CompileError: in.CompileError,
	}).Error; err != nil {
		return nil, errors.New("update Status Failed")
	}
	return &pb.SyncJudgeTaskStatusResponse{}, nil
}

func (s *server) FinishJudgeTask(ctx context.Context, in *pb.FinishJudgeTaskRequest) (*pb.FinishJudgeTaskResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	if !currentUser.Admin {
		return nil, errors.New("permission denied")
	}
	if in.JudgeName == "" {
		return nil, errors.New("JudgeName is empty")
	}
	id := in.SubmissionId

	if err := updateSubmissionRegistration(s.db, id, in.JudgeName, 10*time.Second); err != nil {
		log.Println(err)
		return nil, err
	}

	sub, err := fetchSubmission(s.db, id)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := s.db.Model(&Submission{
		ID: id,
	}).Updates(&Submission{
		Status:    in.Status,
		MaxTime:   int32(in.Time * 1000),
		MaxMemory: in.Memory,
		Hacked:    sub.PrevStatus == "AC" && in.Status != "AC",
	}).Error; err != nil {
		return nil, errors.New("update Status Failed")
	}
	if err := s.db.Model(&Submission{
		ID: id,
	}).Updates(map[string]interface{}{
		"testhash": in.CaseVersion,
	}).Error; err != nil {
		log.Print(err)
		return nil, errors.New("failed to clear judge_name")
	}

	if err := releaseSubmissionRegistration(s.db, id, in.JudgeName); err != nil {
		return nil, errors.New("failed to release Submission")
	}
	return &pb.FinishJudgeTaskResponse{}, nil
}

type Category struct {
	Title    string   `json:"title"`
	Problems []string `json:"problems"`
}

func (s *server) ProblemCategories(ctx context.Context, in *pb.ProblemCategoriesRequest) (*pb.ProblemCategoriesResponse, error) {
	data, err := fetchMetadata(s.db, "problem_categories")
	if err != nil {
		return nil, err
	}
	var categories []Category
	if json.Unmarshal([]byte(data), &categories); err != nil {
		return nil, err
	}

	var result []*pb.ProblemCategory

	for _, c := range categories {
		result = append(result, &pb.ProblemCategory{
			Title:    c.Title,
			Problems: c.Problems,
		})
	}
	return &pb.ProblemCategoriesResponse{
		Categories: result,
	}, nil
}

func (s *server) ChangeProblemCategories(ctx context.Context, in *pb.ChangeProblemCategoriesRequest) (*pb.ChangeProblemCategoriesResponse, error) {
	currentUserName := getCurrentUserName(ctx)
	currentUser, _ := fetchUser(s.db, currentUserName)
	if !currentUser.Admin {
		return nil, errors.New("permission denied")
	}
	var categories []Category
	for _, c := range in.Categories {
		categories = append(categories, Category{
			Title:    c.Title,
			Problems: c.Problems,
		})
	}
	data, err := json.Marshal(categories)
	if err != nil {
		return nil, err
	}
	if err := setMetadata(s.db, "problem_categories", string(data)); err != nil {
		return nil, err
	}
	return &pb.ChangeProblemCategoriesResponse{}, nil
}
