package model

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	gameapiHallpb "open_protos_repo/gameapi/game/hall"
	openHallpb "open_protos_repo/openapi/game/hall"
	cpb "protos_repo/common"
	pb "protos_repo/game/hall"
	domainpb "protos_repo/system/domain"
	"srv_admin/config"
	"srv_admin/config/constant"
	errDef "srv_admin/model/errors"
	"time"

	"git.yj.live/Golang/source"
	"git.yj.live/Golang/source/log"
	"git.yj.live/Golang/source/pkg/database"
	"google.golang.org/grpc/codes"
	"gorm.io/gorm"
)

type GameHall struct {
	database.Model
	Id        int64                    `gorm:"column:id;type:int(11) unsigned;primaryKey;autoIncrement:true" json:"id"`
	HallType  openHallpb.HallGameType  `gorm:"column:hall_type;type:tinyint(4) unsigned;not null" json:"hall_type"`            // 游戏分类
	Code      string                   `gorm:"column:code;type:varchar(32);not null" json:"code"`                              // 大厅code
	Version   string                   `gorm:"column:version;type:varchar(32);not null" json:"version"`                        // 版本号
	Url       string                   `gorm:"column:url;type:varchar(32);not null" json:"url"`                                // 跳转url
	AgencyNum int32                    `gorm:"column:agency_num;type:int(11) unsigned;not null" json:"agency_num"`             // 总社数量
	GameNum   int32                    `gorm:"column:game_num;type:int(11) unsigned;not null" json:"game_num"`                 // 游戏数量
	ParentId  int64                    `gorm:"column:parent_id;type:int(11) unsigned;not null" json:"parent_id"`               // 父id：0是游戏大厅记录，1是总社游戏大厅
	AgencyId  int64                    `gorm:"column:agency_id;type:int(11) unsigned;not null" json:"agency_id"`               // 总社id
	Creator   string                   `gorm:"column:creator;type:varchar(255);not null" json:"creator"`                       // 管理员
	CreatorId int32                    `gorm:"column:creator_id;type:int(11) unsigned;not null" json:"creator_id"`             // 管理员id
	IsDeleted openHallpb.IsDeleted     `gorm:"column:is_deleted;type:tinyint(3) unsigned;not null" json:"is_deleted"`          // 是否删除：0是未删除，1是已删除
	Status    openHallpb.EnableDisable `gorm:"column:status;type:tinyint(3) unsigned;not null;default:1" json:"status"`        // 状态：1是开启，2是关闭
	Sort      int64                    `gorm:"column:status;type:int(11) unsigned;not null;default:0" json:"sort"`             // 排序：越大在前面
	Created   time.Time                `gorm:"column:created;type:datetime;not null;default:CURRENT_TIMESTAMP" json:"created"` // 创建时间
	Updated   time.Time                `gorm:"column:updated;type:datetime;not null;default:CURRENT_TIMESTAMP" json:"updated"` // 更新时间
	Loading   HallLoading              `gorm:"column:loading;type:text;not null;" json:"loading"`                              // loading图配置
}

// NewGameHall 返回
func NewGameHall() *GameHall {
	return &GameHall{}
}

type HallLoading []*openHallpb.HallLoading

// Scan .
func (l *HallLoading) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	v, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("value %v type not string", value)
	}
	return json.Unmarshal(v, l)
}

// Value .
func (l HallLoading) Value() (driver.Value, error) {
	return json.Marshal(l)
}

// TableName GameHall's table name
func (g GameHall) TableName() string {
	return "game_hall"
}

// Create 更新游戏大厅
func (g GameHall) Create(ctx context.Context, in *gameapiHallpb.GameHallInfo) error {
	// 查询code是否被使用
	existsInfo, err := g.getByCode(ctx, &cpb.ReqWithIdStr{Id: in.Code})
	if err != nil {
		return errDef.Errorf(errDef.ERR_CREATE_GAMEHALL,
			codes.NotFound, "create game hall find code error:%s", err.Error())
	}
	if existsInfo == nil || (existsInfo.Id > 0 && existsInfo.IsDeleted == openHallpb.IsDeleted_IS_DELETED_NO) {
		return errDef.Errorf(errDef.ERR_CREATE_GAMEHALL,
			codes.AlreadyExists, "create game hall error,code is exists, code:%s", in.Code)
	}

	err = g.SetData(func() error {
		data := g.formatData(in)
		db := config.NewDB()
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := db.Model(&GameHall{}).Create(data).Error; err != nil {
				// 先插入大厅表
				log.L().Errorc(ctx, "create game hall error:%s", err.Error())
				return errDef.Errorf(errDef.ERR_CREATE_GAMEHALL,
					codes.Internal, "create game hall error:%s", err.Error())
			}
			// 将插入id带回去
			in.Id = data.Id
			// 一条大厅游戏对应多个总社写入到同个表，通过parent_id字段区分
			childData := g.formatChildData(in)
			if err := db.Model(&GameHall{}).Create(&childData).Error; err != nil {
				log.L().Errorc(ctx, "create agency game hall error:%s", err.Error())
				return errDef.Errorf(errDef.ERR_CREATE_GAMEHALL,
					codes.Internal, "create agency game hall error:%s", err.Error())
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}, g.getTableCache())

	return err
}

// Update  更新游戏大厅
func (g GameHall) Update(ctx context.Context, in *gameapiHallpb.GameHallInfo) error {
	// 查找是否存在
	existsInfo, err := g.Get(ctx, &cpb.ReqWithId{Id: in.Id})
	if err != nil || existsInfo == nil || existsInfo.Id <= 0 ||
		existsInfo.ParentId > 0 || existsInfo.IsDeleted == openHallpb.IsDeleted_IS_DELETED_YES {
		return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL, codes.NotFound, "game hall no exists, id:%d", in.Id)
	}
	// 先更新
	err = g.SetData(func() error {
		db := config.NewDB()
		if err := db.Transaction(func(tx *gorm.DB) error {
			loadingStr, _ := json.Marshal(in.Loading)
			updateFiled := map[string]interface{}{
				"loading":    loadingStr,
				"version":    in.Version,
				"url":        in.Url,
				"hall_type":  in.HallType,
				"agency_num": len(in.AgencyId),
				"updated":    time.Now(),
			}
			// 先更新游戏大厅
			if err := db.Model(&GameHall{}).Where("id=?", in.Id).Updates(updateFiled).Error; err != nil {
				return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL, codes.Internal,
					"update game hall error, id:%d", in.Id)
			}

			// 删除旧数据
			if err := db.Where("parent_id=?", in.Id).
				Delete(&GameHall{}).Error; err != nil {
				return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL, codes.Internal,
					"delete agency game hall error, parent_id:%d", in.Id)
			}

			// 一条大厅游戏对应多个总社写入到同个表，通过parent_id字段区分
			childData := g.formatChildData(in)
			if err := db.Model(&GameHall{}).Create(&childData).Error; err != nil {
				log.L().Errorc(ctx, "create agency game hall error:%s", err.Error())
				return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL,
					codes.Internal, "create agency game hall error:%s", err.Error())
			}

			return nil
		}); err != nil {
			return err
		}
		return nil
	}, g.getByIdCache(&cpb.ReqWithId{Id: in.Id}))

	return err
}

// UpdateStatus 更新游戏大厅状态更新
func (g GameHall) UpdateStatus(ctx context.Context, in *pb.UpdateStatusReq) error {
	// 判断是否存在
	existsInfo, err := g.Get(ctx, &cpb.ReqWithId{Id: in.Id})
	if err != nil {
		return err
	}
	if existsInfo == nil || existsInfo.Id <= 0 || existsInfo.ParentId > 0 ||
		existsInfo.IsDeleted == openHallpb.IsDeleted_IS_DELETED_YES {
		return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL, codes.Internal, "game hall no exists,id:%d", in.Id)
	}
	err = g.SetData(func() error {
		updateFiled := map[string]interface{}{"status": in.Status, "updated": time.Now()}
		if err := config.NewDB().Model(&GameHall{}).Where("id=?", in.Id).
			Updates(updateFiled).Error; err != nil {
			log.L().Errorc(ctx, "update game hall status error:%s, id:%d", err.Error(), in.Id)
			return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL, codes.Internal, "update game hall status error,id:%d", in.Id)
		}
		if err := config.NewDB().Model(&GameHall{}).Where("parent_id=?", in.Id).
			Updates(updateFiled).Error; err != nil {
			log.L().Errorc(ctx, "update agency game hall status error:%s, parent_id:%d", err.Error(), in.Id)
			return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL, codes.Internal,
				"update agency game hall status error,parent_id:%d", in.Id)
		}
		return nil
	}, g.getByIdCache(&cpb.ReqWithId{Id: in.Id}))
	return err
}

// Delete 删除游戏大厅
func (g GameHall) Delete(ctx context.Context, in *cpb.ReqWithId) error {
	// 判断是否存在
	existsInfo, err := g.Get(ctx, &cpb.ReqWithId{Id: in.Id})
	if err != nil {
		return err
	}
	if existsInfo == nil || existsInfo.Id <= 0 || existsInfo.ParentId > 0 ||
		existsInfo.IsDeleted == openHallpb.IsDeleted_IS_DELETED_YES {
		return errDef.Errorf(errDef.ERR_DELETE_GAMEHALL, codes.Internal, "game hall no exists,id:%d", in.Id)
	}
	err = g.SetData(func() error {
		updateFiled := map[string]interface{}{"is_deleted": openHallpb.IsDeleted_IS_DELETED_YES, "updated": time.Now()}
		db := config.NewDB()
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := db.Model(&GameHall{}).Where("id=?", in.Id).
				Updates(updateFiled).Error; err != nil {
				log.L().Errorc(ctx, "delete game hall error,id:%d", in.Id)
				return errDef.Errorf(errDef.ERR_DELETE_GAMEHALL, codes.Internal,
					"delete game hall error,id:%d", in.Id)
			}
			if err := db.Model(&GameHall{}).Where("parent_id=?", in.Id).Updates(updateFiled).Error; err != nil {
				log.L().Errorc(ctx, "delete agency game hall error,parent_id:%d", in.Id)
				return errDef.Errorf(errDef.ERR_DELETE_GAMEHALL, codes.Internal,
					"delete agency game hall error,parent_id:%d", in.Id)
			}
			return nil
		}); err != nil {
			return err
		}
		return nil

	}, g.getByIdCache(in))
	return err
}

// Search 查询列表
func (g GameHall) Search(ctx context.Context, in *openHallpb.SearchHall) (*gameapiHallpb.GameHallList, error) {
	var result gameapiHallpb.GameHallList
	_, err := g.GetData(&result, g.getSearchCache(in), func() (interface{}, error) {
		var gameHall []GameHall
		db := config.NewDB()
		if err := db.Model(&GameHall{}).Scopes(
			g.searchByAgencyId(in.AgencyId),
			g.searchByType(in.Type),
			g.searchById(in.Id),
			g.searchByStatus(in.Status),
			g.searchByParentId(in.ParentId),
			g.searchByIsDeleted(in.IsDeleted),
			g.searchByCode(in.Code),
			WithPage(&cpb.ReqWithPage{Page: in.Page, Size: in.Size}),
			withSort(in.Sort),
		).Find(&gameHall).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, nil
			}
			log.L().Errorc(ctx, "search game hall list error:%s, in:%+v", err.Error(), in)
			return &gameapiHallpb.GameHallList{}, errDef.Errorf(errDef.ERR_FIND_GAMEHALL,
				codes.Internal, "search game hall list error")
		}
		if len(gameHall) == 0 {
			return &gameapiHallpb.GameHallList{}, nil
		}
		var data gameapiHallpb.GameHallList
		if err := db.Table(g.TableName()).Scopes(
			g.searchByAgencyId(in.AgencyId),
			g.searchByType(in.Type),
			g.searchById(in.Id),
			g.searchByStatus(in.Status),
			g.searchByParentId(in.ParentId),
			g.searchByIsDeleted(in.IsDeleted),
			g.searchByCode(in.Code)).
			Count(&data.Total).Error; err != nil {
			log.L().Errorc(ctx, "search game hall count error:%s, in:%+v", err.Error(), in)
			return &gameapiHallpb.GameHallList{}, errDef.Errorf(errDef.ERR_FIND_GAMEHALL,
				codes.Internal, "search game hall count error")
		}

		cdnUrl := (&DomainPool{}).SelectUrlWithRandom(&domainpb.SearchReq{
			Status:  cpb.Bool_TRUE.Enum(),
			Type:    domainpb.DomainType_DT_PLATFORM_CDN,
			BrandId: -1,
		})

		for k := range gameHall {
			for k2 := range gameHall[k].Loading {
				if gameHall[k].Loading[k2].ImageH != "" {
					gameHall[k].Loading[k2].ImageH = cdnUrl + gameHall[k].Loading[k2].ImageH
				}
				if gameHall[k].Loading[k2].ImageS != "" {
					gameHall[k].Loading[k2].ImageS = cdnUrl + gameHall[k].Loading[k2].ImageS
				}
				if gameHall[k].Loading[k2].Icon != "" {
					gameHall[k].Loading[k2].Icon = cdnUrl + gameHall[k].Loading[k2].Icon
				}
			}

			data.List = append(data.List, gameHall[k].info())
		}
		return &data, nil
	})
	if err != nil {
		return &gameapiHallpb.GameHallList{}, err
	}

	// 获取游戏大厅名称-多语言
	for k := range result.List {
		for k2 := range result.List[k].Loading {
			if result.List[k].Loading[k2].Name == "" {
				continue
			}
			if constant.DefaultLanguage == result.List[k].Loading[k2].Language {
				result.List[k].Name = result.List[k].Loading[k2].Name
				if constant.DefaultLanguage == in.Language {
					break
				}
			}
			if in.Language == result.List[k].Loading[k2].Language {
				result.List[k].Name = result.List[k].Loading[k2].Name
				break
			}
		}
	}

	result.Page = in.Page + 1
	result.Size = in.Size
	return &result, nil
}

// GetByCode 根据code查询游戏大厅详情
func (g GameHall) GetByCode(ctx context.Context, in *cpb.ReqWithIdStr) (*gameapiHallpb.GameHallInfo, error) {
	var result gameapiHallpb.GameHallInfo
	_, err := g.GetData(&result, g.getByCodeCache(in), func() (interface{}, error) {
		return g.getByCode(ctx, in)
	})
	return &result, err
}

// GetByCodeAgencyId 根据code和agencyId查询
func (g GameHall) GetByCodeAgencyId(ctx context.Context, in *gameapiHallpb.HallInfoReq) (*openHallpb.HallInfo, error) {
	var result openHallpb.HallInfo
	_, err := g.GetData(&result, g.getByCodeAgencyIdCache(in), func() (interface{}, error) {
		var data GameHall
		if err := config.NewDB().Model(&GameHall{}).Scopes(
			g.searchByCode(in.Code),
			g.searchByAgencyId(in.AgencyId),
			g.searchByIsDeleted(openHallpb.IsDeleted_IS_DELETED_NO),
			g.searchByStatus(openHallpb.EnableDisable_ENABLED),
		).First(&data).Error; err != nil {
			log.L().Errorc(ctx, "game hall no exists,err:%s, code:%s,agency_id:%d", err.Error(), in.Code, in.AgencyId)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 强制不写入缓存
				return nil, nil
			}
			return &gameapiHallpb.GameHallInfo{}, errDef.Errorf(errDef.ERR_FIND_GAMEHALL,
				codes.Internal, "game hall no exists,code:%s,agency_id:%d", in.Code, in.AgencyId)
		}
		return data.gameApiInfo(), nil
	})

	return &result, err
}

// getByCode 通过code查询
func (g GameHall) getByCode(ctx context.Context, in *cpb.ReqWithIdStr) (*gameapiHallpb.GameHallInfo, error) {
	var gameHallInfo GameHall
	if err := config.NewDB().Model(&GameHall{}).Scopes(
		g.searchByCode(in.Id),
		g.searchByParentId(0),
		g.searchByIsDeleted(openHallpb.IsDeleted_IS_DELETED_NO),
	).First(&gameHallInfo).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &gameapiHallpb.GameHallInfo{}, nil
		}
		log.L().Errorc(ctx, "get game hall by code error:%s, code:%s", err.Error(), in.Id)
		return &gameapiHallpb.GameHallInfo{}, errDef.Errorf(errDef.ERR_FIND_GAMEHALL,
			codes.Internal, "get game hall by code error.code:%s", in.Id)
	}
	return gameHallInfo.info(), nil
}

// Get 查询某条数据
func (g GameHall) Get(ctx context.Context, in *cpb.ReqWithId) (*gameapiHallpb.GameHallInfo, error) {
	var result gameapiHallpb.GameHallInfo
	_, err := g.GetData(&result, g.getByIdCache(in), func() (interface{}, error) {
		var gameHallInfo GameHall
		if err := config.NewDB().Model(&GameHall{}).Scopes(g.searchById(in.Id)).First(&gameHallInfo).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return &gameapiHallpb.GameHallInfo{}, nil
			}
			log.L().Errorc(ctx, "get game hall error:%s, id:%d", err.Error(), in.Id)
			return &gameapiHallpb.GameHallInfo{}, errDef.Errorf(errDef.ERR_FIND_GAMEHALL,
				codes.Internal, "get game hall error.id:%d", in.Id)
		}
		return gameHallInfo.info(), nil
	})
	if err != nil {
		return &gameapiHallpb.GameHallInfo{}, err
	}
	return &result, nil

}

// UpdateGameNum 更新游戏大厅的游戏数量
func (g GameHall) UpdateGameNum(ctx context.Context, tx *gorm.DB, id int64, gameNum int64) error {
	if tx == nil {
		tx = config.NewDB()
	}
	err := g.SetData(func() error {
		if err := tx.Model(&GameHall{}).Where("id=?", id).Update("game_num", gameNum).Error; err != nil {
			log.L().Errorc(ctx, "update game hall game_num error:%s,id:%d.", err.Error(), id)
			return errDef.Errorf(errDef.ERR_UPDATE_GAMEHALL,
				codes.Internal, "update game hall game_num error,id:%d.", id)
		}
		return nil
	}, g.getByIdCache(&cpb.ReqWithId{Id: id}))

	return err
}

// GetHallAgencyIds 获取游戏大厅关联总社ids
func (g GameHall) GetHallAgencyIds(ctx context.Context, in *cpb.ReqWithId) (*cpb.ReqWithIdList, error) {
	var data []GameHall
	if err := config.NewDB().Model(&GameHall{}).Scopes(
		g.searchByParentId(in.Id),
	).Find(&data).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &cpb.ReqWithIdList{}, nil
		}
		log.L().Errorc(ctx, "get game hall agency id list error:%s. hall_id:%d", err.Error(), in.Id)
		return &cpb.ReqWithIdList{}, errDef.Errorf(errDef.ERR_FIND_GAMEHALL,
			codes.Internal, "get game hall agency id list error. hall_id:%d", in.Id)
	}

	var result cpb.ReqWithIdList
	result.Id = make([]int64, 0)
	for k := range data {
		result.Id = append(result.Id, data[k].AgencyId)
	}
	return &result, nil
}

// info 数据库信息转为proto
func (g GameHall) info() *gameapiHallpb.GameHallInfo {
	return &gameapiHallpb.GameHallInfo{
		Id:        g.Id,
		HallType:  g.HallType,
		Code:      g.Code,
		Version:   g.Version,
		Url:       g.Url,
		AgencyNum: g.AgencyNum,
		GameNum:   g.GameNum,
		Creator:   g.Creator,
		CreatorId: g.CreatorId,
		IsDeleted: g.IsDeleted,
		Status:    g.Status,
		Created:   g.Created.Format(source.Date_YYYYMMDDHHMMSS),
		Updated:   g.Updated.Format(source.Date_YYYYMMDDHHMMSS),
		ParentId:  g.ParentId,
		Loading:   g.Loading,
	}
}

// info 数据库信息转为game_api proto
func (g GameHall) gameApiInfo() *openHallpb.HallInfo {
	return &openHallpb.HallInfo{
		Id:          g.Id,
		HallType:    g.HallType,
		Code:        g.Code,
		Version:     g.Version,
		Url:         g.Url,
		GameNum:     g.GameNum,
		LoadingList: g.Loading,
		Loading:     &openHallpb.HallLoading{},
		HallId:      g.ParentId, // 游戏大厅id
	}
}

// formatData 将proto的信息转为数据库表信息-游戏大厅记录
func (g GameHall) formatData(data *gameapiHallpb.GameHallInfo) *GameHall {
	// 处理图片 nil
	if data.Loading == nil {
		data.Loading = []*openHallpb.HallLoading{}
	}
	return &GameHall{
		Id:        data.Id,
		HallType:  data.HallType,
		Code:      data.Code,
		Url:       data.Url,
		AgencyNum: int32(len(data.AgencyId)),
		Creator:   data.Creator,
		CreatorId: data.CreatorId,
		Created:   time.Now(),
		Updated:   time.Now(),
		ParentId:  0,
		AgencyId:  0,
		Loading:   data.Loading,
		Version:   data.Version,
	}
}

// formatChildData 生成总社游戏大厅记录
func (g GameHall) formatChildData(in *gameapiHallpb.GameHallInfo) []GameHall {
	data := make([]GameHall, 0)
	// 处理图片 nil
	if in.Loading == nil {
		in.Loading = []*openHallpb.HallLoading{}
	}
	for k := range in.AgencyId {
		data = append(data, GameHall{
			HallType:  in.HallType,
			Code:      in.Code,
			Url:       in.Url,
			AgencyNum: int32(len(in.AgencyId)),
			GameNum:   0,
			ParentId:  in.Id,
			AgencyId:  in.AgencyId[k],
			Creator:   in.Creator,
			CreatorId: in.CreatorId,
			Created:   time.Now(),
			Updated:   time.Now(),
			Loading:   in.Loading,
			Version:   in.Version,
		})
	}
	return data
}

// searchById 根据id查询
func (g GameHall) searchById(id int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if id > 0 {
			return db.Where("id=?", id)
		}
		return db
	}
}

// searchByType 根据大厅分类查询
func (g GameHall) searchByType(hallType openHallpb.HallGameType) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if hallType > 0 {
			return db.Where("hall_type=?", hallType)
		}
		return db
	}
}

// searchByHallId 根据总社id查询
func (g GameHall) searchByHallId(hallId int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if hallId > 0 {
			return db.Where("hall_id=?", hallId)
		}
		return db
	}
}

// searchByAgencyId 根据总社id查询
func (g GameHall) searchByAgencyId(agencyId int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if agencyId > 0 {
			return db.Where("agency_id=?", agencyId)
		}
		return db
	}
}

// searchByParentId 根据parent_id查询
func (g GameHall) searchByParentId(parentId int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if parentId < 0 {
			return db.Where("parent_id>?", 0)
		} else {
			return db.Where("parent_id=?", parentId)
		}
	}
}

// searchByIsDeleted 根据删除状态查询:0是未删除，1是已删除
func (g GameHall) searchByIsDeleted(isDeleted openHallpb.IsDeleted) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if isDeleted >= 0 {
			return db.Where("is_deleted=?", isDeleted)
		}
		return db
	}
}

// searchByStatus 根据status查询
func (g GameHall) searchByStatus(status openHallpb.EnableDisable) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if status > 0 {
			return db.Where("status=?", status)
		}
		return db
	}
}

// searchByCode 根据code查询
func (g GameHall) searchByCode(code string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if len(code) > 0 {
			return db.Where("code=?", code)
		}
		return db
	}
}

// searchByGameId 根据game_id查询
func (g GameHall) searchByGameId(gameId int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if gameId > 0 {
			return db.Where("game_id=?", gameId)
		}
		return db
	}
}

// getByIdCache 根据id设置缓存
func (g GameHall) getByIdCache(in *cpb.ReqWithId) *database.Cache {
	return database.NewKeyValueCache(fmt.Sprintf("%s:get_by_id:%d", g.TableName(), in.Id)).
		WithRDS(config.NewRDS()).
		WithClassify(g.classifyCache())
}

// getByCodeCache 根据code设置缓存
func (g GameHall) getByCodeCache(in *cpb.ReqWithIdStr) *database.Cache {
	return database.NewKeyValueCache(fmt.Sprintf("%s:get_by_code:%s", g.TableName(), in.Id)).
		WithRDS(config.NewRDS()).
		WithClassify(g.classifyCache())
}

// getSearchCache 搜索列表缓存
func (g GameHall) getSearchCache(in *openHallpb.SearchHall) *database.Cache {
	return database.NewKeyValueCache(fmt.Sprintf("%s:get_search:%d:%d:%d:%d:%d:%d:%d:%s",
		g.TableName(),
		in.Id, in.Page, in.Size, in.AgencyId, in.Status, in.Type, in.ParentId, in.Code)).
		WithRDS(config.NewRDS()).
		WithClassify(g.classifyCache())
}

// getByCodeAgencyIdCache 搜索列表缓存
func (g GameHall) getByCodeAgencyIdCache(in *gameapiHallpb.HallInfoReq) *database.Cache {
	return database.NewKeyValueCache(fmt.Sprintf("%s:get_code_agency:%s:%d",
		g.TableName(),
		in.Code, in.AgencyId)).
		WithRDS(config.NewRDS()).
		WithClassify(g.classifyCache())
}

// getTableCache 获取缓存表key
func (g GameHall) getTableCache() *database.Cache {
	return database.NewKeyValueCache("").
		WithRDS(config.NewRDS()).
		WithClassify(g.classifyCache())
}

// classifyCache 设置缓存表
func (g GameHall) classifyCache() string {
	return fmt.Sprintf("%s:cache", g.TableName())
}
