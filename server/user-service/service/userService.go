package service

import (
	"context"
	"errors"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log"
	"main.go/data"
	"main.go/repository"
	"main.go/utils"
)

type UserService struct {
	user   *repository.UserRepository
	cache  *repository.UserCache
	logger *log.Logger
	tracer trace.Tracer
}

type Project struct {
	ID      string   `json:"id"`
	UserIDs []string `bson:"user_ids" json:"user_ids"`
}

func NewUserService(user *repository.UserRepository, cache *repository.UserCache, logger *log.Logger, trace trace.Tracer) *UserService {
	return &UserService{user, cache, logger, trace}
}

func (s *UserService) Registration(ctx context.Context, request *data.AccountRequest) error {
	ctx, span := s.tracer.Start(ctx, "UserService.Registration")
	defer span.End()
	err := s.cache.Register(ctx, request)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful registration")
	return nil

}

func (s *UserService) GetAll(ctx context.Context) (*data.Accounts, error) {
	ctx, span := s.tracer.Start(ctx, "UserService.GetAll")
	defer span.End()
	managers, err := s.user.GetAllManagers(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successful get all managers")
	return &managers, nil

}

func (s *UserService) GetOne(ctx context.Context, userId string) (*data.Account, error) {
	ctx, span := s.tracer.Start(ctx, "UserService.GetOne")
	defer span.End()
	manager, err := s.user.GetOne(ctx, userId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successful get one manager")
	return manager, nil

}

func (s *UserService) GetAllMembers(ctx context.Context) ([]data.Account, error) {
	ctx, span := s.tracer.Start(ctx, "UserService.GetAllMembers")
	defer span.End()
	accounts, err := s.user.GetAllMembers(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successful get all members")
	return accounts, nil
}

func (s UserService) Login(ctx context.Context, user *data.LoginCredentials) (id string, role string, token string, err error) {
	ctx, span := s.tracer.Start(ctx, "UserService.Login")
	defer span.End()
	role, err = s.user.GetUserRoleByEmail(ctx, user.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", "", errors.New("role does not exist")
	}
	get, err := s.user.GetUserIdByEmail(ctx, user.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", "", errors.New("error getting user")
	}
	token, err = utils.CreateToken(user.Email, role, get.Hex())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", "", errors.New("error creating token")
	}
	err = s.cache.Login(ctx, user, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", "", err
	}

	span.SetStatus(codes.Ok, "Successful login")
	return get.Hex(), role, token, nil
}

func (s *UserService) Logout(ctx context.Context, id string) error {
	ctx, span := s.tracer.Start(ctx, "UserService.Logout")
	defer span.End()
	err := s.cache.Logout(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful logout")
	return nil

}

func (s *UserService) PasswordCheck(ctx context.Context, id string, password string) bool {
	return s.user.CheckIfPasswordIsSame(ctx, id, password)
}

func (s *UserService) ChangePassword(ctx context.Context, id string, password string) error {
	ctx, span := s.tracer.Start(ctx, "UserService.ChangePassword")
	defer span.End()
	err := s.user.ChangePassword(ctx, id, password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful change password")
	return nil
}

func (s *UserService) RecoveryRequest(ctx context.Context, email string) error {
	ctx, span := s.tracer.Start(ctx, "UserService.RecoveryRequest")
	defer span.End()
	err := s.user.HandleRecoveryRequest(ctx, email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful recovery request")
	return nil
}

func (s *UserService) ResettingPassword(ctx context.Context, email string, password string) error {
	_, span := s.tracer.Start(context.Background(), "UserService.ResettingPassword")
	defer span.End()
	err := s.user.ResetPassword(ctx, email, password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful resetting password")
	return nil
}

func (s *UserService) MagicLink(ctx context.Context, email string) error {
	ctx, span := s.tracer.Start(ctx, "UserService.MagicLink")
	defer span.End()
	err := s.cache.ImplementMagic(ctx, email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful magic link")
	return nil
}

func (s *UserService) VerifyMagic(ctx context.Context, email string) (string, string, error) {
	ctx, span := s.tracer.Start(ctx, "UserService.VerifyMagic")
	defer span.End()
	id, token, err := s.cache.VerifyMagic(ctx, email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", err
	}
	span.SetStatus(codes.Ok, "Successful verify magic")
	return id, token, nil
}

func (us *UserService) ValidateToken(ctx context.Context, token string) (string, string, error) {
	ctx, span := us.tracer.Start(ctx, "UserService.ValidateToken")
	defer span.End()
	userID, err := us.cache.VerifyTokenWithUserId(ctx, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		us.logger.Println("Error verifying token:", err)
		return "", "", err
	}
	if userID == "" {
		span.RecordError(errors.New("invalid token"))
		span.SetStatus(codes.Error, "Invalid token")
		return "", "", errors.New("invalid token")
	}
	role, err := us.cache.GetUserRole(ctx, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		us.logger.Println("Error retrieving user role:", err)
		return "", "", err
	}
	span.SetStatus(codes.Ok, "Successful validate token")
	return userID, role, nil
}

func (us *UserService) Delete(ctx context.Context, userId string) error {
	ctx, span := us.tracer.Start(ctx, "UserService.Delete")
	defer span.End()
	err := us.user.Delete(ctx, userId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successful delete user")
	return nil
}
func (us *UserService) VerifyRecaptcha(ctx context.Context, token string) (bool, error) {
	ctx, span := us.tracer.Start(ctx, "UserService.VerifyRecaptcha")

	defer span.End()
	success, err := us.user.VerifyRecaptcha(ctx, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		us.logger.Println("Error verifying recaptcha:", err)
		return false, err
	}
	span.SetStatus(codes.Ok, "Successful verify recaptcha")
	return success, nil
}

func (us *UserService) GetUsersByIds(ctx context.Context, userIds []string) ([]data.Account, error) {
	ctx, span := us.tracer.Start(ctx, "UserService.GetUsersByIds")
	defer span.End()
	users, err := us.user.GetUsersByIds(ctx, userIds)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successful get users")
	return users, nil
}

func (us *UserService) GetRoleForMagic(ctx context.Context, email string) (string, error) {
	role, err := us.user.GetRoleForMagic(ctx, email)
	if err != nil {
		return "", err
	}
	return role, nil
}

func (us *UserService) VerifyAccount(ctx context.Context, email string) error {
	err := us.cache.VerifyAccount(ctx, email)
	if err != nil {
		return err
	}
	return nil
}
